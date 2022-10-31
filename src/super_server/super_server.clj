(ns super-server.super-server
  (:require [fbc-utils.core :as ut]
            [fbc-utils.debug :refer [let-dbg]]
            [snek.core :as sn]
            [com.walmartlabs.lacinia.schema :as sc]
            [com.walmartlabs.lacinia.util :as lu]
            [com.walmartlabs.lacinia.resolve :as lr]
            [com.walmartlabs.lacinia :as la]
            [com.walmartlabs.lacinia.pedestal :as lp]
            [io.pedestal.http.route :as ro]
            [io.pedestal.http :as ht]
            [io.pedestal.http.ring-middlewares :as rm]
            [clojure.java.browse :refer [browse-url]]
            [datahike.api :as dh]
            [taoensso.timbre :as ti]
            [ring.middleware.session.cookie :as co]
            [clj-pid.core :as pid]
            [clojure.set :as se]
            [clojure.string :as st]
            [clojure.pprint :as pp]
            [super-server.user-accounts :as ua]
            [backtick :as bt]))

(ti/set-level! :warn)

(defonce db (atom nil))

(defn datahike-schema [schema]
  (for [[k v] schema]
    (merge {:db/ident k}
           (if-let [[_ s] (re-matches #"(.*)-many" (name v))]
             {:db/valueType (keyword (str "db.type/" s))
              :db/cardinality :db.cardinality/many}
             {:db/valueType (keyword (str "db.type/" (name v)))
              :db/cardinality :db.cardinality/one}))))

(defn empty-temp-database! [db-schema]
  (let [id         (str "x" (rand-int 1000000))
        config     {:store      {:backend :mem
                                 :id      id}
                    :initial-tx (datahike-schema db-schema)}]
    (when-not (dh/database-exists? config)
      (dh/create-database config))
    (reset! db (dh/connect config))))

(defn permanent-database! [db-schema fname]
  (let [config        {:store {:backend :file
                               :path    (or fname "database")}}
        new-database? (not (dh/database-exists? config))]
    (when new-database?
      (dh/create-database config))
    (reset! db (dh/connect config))
    (dh/transact @db (datahike-schema db-schema))
    :ready))

(defn unbox-entity [e]
  (if (associative? e)
    (:db/id e)
    e))

(defn datom-exists? [[e a v]]
  (= (unbox-entity (a (dh/entity @@db e))) (unbox-entity v)))

(defn query-helper [& q]
  (if-let [labels (seq (distinct (filter (fn [x]
                                           (and (symbol? x) (= (first (name x)) \?)))
                                         (flatten q))))]
    (seq (dh/q `[:find  ~@labels
                 :where ~@q]
               @@db))
    (every? datom-exists? q)))

(defmacro query [& q]
  `(apply query-helper (bt/template ~q)))

(defn resolver-names [item]
  (if (map? item)
    (if-let [resolve (:resolve item)]
      [resolve]
      (mapcat resolver-names (vals item)))
    []))

(defn fix-resolvers [graphql-schema]
  (let [fix-resolver (fn [{:keys [resolve]
                           :as   field}]
                       (cond-> field
                         resolve (update :resolve
                                         (fn [fun]
                                           (fn [context args value]
                                             (try (fun @db context args value)
                                                  (catch Exception e
                                                    (println (str (type e) ":" (ex-message e)))
                                                    (lr/resolve-as nil {:message (str (type e) ":" (ex-message e))}))))))))]
    (sn/modify {:objects   {nil {:fields {nil fix-resolver}}}
                :queries   {nil fix-resolver}
                :mutations {nil fix-resolver}}
               graphql-schema)))

(defn current-userid [context]
  (when-let [id (ua/session-user-id context)]
    (:userid (ua/user-by-id @db id))))

(defn eid-by-userid [userid]
  (let [[[eid]] (query-helper '[?e :user/userid userid])]
    eid))

(defn gather-entities-with-atts [atts]
  (reduce (fn [acc item]
            (reduce (fn [acc2 [eid val :as item2]]
                      (update acc2 eid assoc item val))
                    acc
                    (query [?e ~item ?v])))
          {}
          atts))

(defn relation-tree [relations]
  (loop [result     []
         relations  relations
         num-misses 0]
    (let [{:keys [more
                  found
                  result]} (reduce (fn [{:keys [more
                                                found
                                                result]
                                         :as   acc}
                                        [id dependencies :as item]]
                                     (if (seq dependencies)
                                       (if-let [[path] (seq (filter (fn [path]
                                                                      (<= (count (se/difference dependencies (set path))) num-misses))
                                                                    (conj result [])))]
                                         {:more   more
                                          :found  true
                                          :result (conj result (conj path id))}
                                         {:more   (conj more item)
                                          :found  found
                                          :result result})
                                       {:more   more
                                        :found  true
                                        :result (conj result [id])}))
                                   {:more   []
                                    :found  false
                                    :result result}
                                   relations)]
      (if (seq more)
        (recur result
               more
               (if found
                 0
                 (inc num-misses)))
        (reduce (fn fun [acc [cur & more :as item]]
                  (if (seq more)
                    (assoc acc cur (fun (acc cur) more))
                    (assoc acc cur {})))
                {}
                result)))))

;;(relation-tree {1 #{} 2 #{1} 3 #{2 1} 4 #{2} 5 #{} 6 #{2 5} 7 #{4}})

(defn database-dump []
  (let [attributes   (into {}
                           (map (comp vec rest)
                                (query [?e :db/ident ?n]
                                       [?e :db/valueType ?v])))
        entity-names (set (map namespace (map first attributes)))
        entities     (gather-entities-with-atts (keys attributes))
        relations    (for [[eid entity] entities]
                       [eid
                        (set (filter identity
                                     (for [[k v] entity]
                                       (when (and (= :db.type/ref (attributes k)) (entities v))
                                         (when-let [[_ k] (re-matches #"^(.+)-id$" (name k))]
                                           (when (entity-names k)
                                             v))))))])
        tree         (relation-tree (reverse (sort-by first relations)))]
    (letfn [(grouped-chis [coll]
              (into {}
                    (for [[group members] (group-by (fn [[chi-id]]
                                                      (namespace (first (keys (entities chi-id)))))
                                                    coll)]
                      [(keyword "dump" group) (map fun (sort-by first members))])))
            (fun [[id chis]]
              (into {}
                    (concat (for [[k v] (entities id)]
                              [(keyword (name k)) v])
                            [[:id id]]
                            (grouped-chis chis))))]
      (grouped-chis tree))))

(defn database-dump-pp []
  (let [prindent (fn [indent & more]
                   (println (apply str (apply str (repeat indent "  ")) more)))
        data     (database-dump)]
    (letfn [(pp-items [indent typ items]
              (if (or (= (count items) 1)
                      (some (fn [att]
                              (= (namespace att) "dump"))
                            (mapcat keys items)))
                (do (doseq [item items]
                      (prindent (dec indent) "--" (st/upper-case (name typ)) " " (:id item))
                      (pp-item indent item)))
                (do (prindent (dec indent) "--" (st/upper-case (name typ)) "S")
                    (doseq [line (rest (st/split (with-out-str (pp/print-table items)) #"\n"))]
                      (prindent indent line)))))
            (pp-item [indent item]
              (let [[dumps atoms] (ut/partition-pred (fn [[k v]]
                                                       (= (namespace k) "dump"))
                                                     (dissoc item :id))]
                (doseq [[k v] (sort-by (comp name first) atoms)]
                  (prindent indent (name k) " = " v))
                (doseq [[k v] dumps]
                  (pp-items (inc indent) k v))))]
      (pp-item 0 data))))

(defonce server (atom nil))

(def atomize-session-interceptor
  {:name  ::atomize-session
   :enter (fn [context]
            (let [session (:session (:request context))]
              (assoc-in context [:request :session-response] (atom session))))
   :leave (fn [context]
            (let [session (:session-response (:request context))]
              (assoc-in context [:response :session] @session)))})

(def mock-session-interceptor
  {:name  ::fake-session
   :enter (fn [context]
            (let [eid (eid-by-userid "drcode")]
              (assoc-in context
                        [:request :session]
                        {:id (if eid
                               (str "user:" eid)
                               "user:115")})))})

(defn inject [interceptors interceptor interceptor-before-name]
  (reduce (fn [acc {:keys [name]
                    :as   item}]
            (if (= name interceptor-before-name)
              (conj acc interceptor item)
              (conj acc item)))
          []
          interceptors))

(defn respond-greet [request]
  {:status 200
   :body   (str "Hello superserver server " (pid/current))})

(defn start-server [{:keys [schema
                            local-react?
                            port
                            user-accounts?
                            index-fun]
                     :as options}]
  (assert (empty? (se/difference #{:schema
                                   :local-react?
                                   :port
                                   :user-accounts?}
                                 (set (keys options)))))
  (assert @db)
  (let [session-interceptor (rm/session {:store        (co/cookie-store)
                                         :cookie-attrs {:max-age 2000000}})
        schema              (cond-> schema
                              user-accounts? ua/attach-user-account-schema)
        schema              (sc/compile (fix-resolvers schema))
        servmap             (cond-> (lp/service-map schema
                                                    (when false 
                                                      {:graphiql true}))
                              true               (merge {::ht/allowed-origins (constantly true)
                                                         ::ht/port            port
                                                         ::ht/host            "0.0.0.0"})
                              (not local-react?) (assoc ::ht/resource-path "public")
                              true               (update :io.pedestal.http/routes
                                                         (partial map
                                                                  (fn [{:keys [path
                                                                               interceptors]
                                                                        :as   route}]
                                                                    (cond-> route
                                                                      (= path "/graphql") (update :interceptors
                                                                                                  (fn [interceptors]
                                                                                                    (cond-> interceptors
                                                                                                      true                              (inject session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context)
                                                                                                      (and local-react? user-accounts?) (inject mock-session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context)
                                                                                                      true                              (inject atomize-session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context))))))))
                              true               (update :io.pedestal.http/routes concat (ro/expand-routes #{["/greet" :get `respond-greet]}))
                              index-fun          (update :io.pedestal.http/routes concat (ro/expand-routes #{["/" :get index-fun]}))
                              #_#_index-fun          (update :io.pedestal.http/routes
                                                             (partial map
                                                                      (fn [{:keys [path]
                                                                            :as   route}]
                                                                        (if (= path "/")
                                                                          #d (first (ro/expand-routes #{["/" :get index-fun]}))
                                                                          route)))))
        existing-server     (boolean @server)]
    (when existing-server
      (ht/stop @server))
    (reset! server (ht/start (ht/create-server servmap)))
    :started))

(defn parse-id [id]
  (when-let [[_ typ eid](re-matches #"^(.+):(\d+)$" id)]
    [typ (Integer. eid)]))

(defn eid->cursor [eid]
  (str "c" eid))

(defn cursor->eid [eid]
  (Integer. (apply str (rest eid))))

(defn add-object [obj]
  (let [{:keys [tempids]} (dh/transact @db
                                       (filter identity
                                               (for [[k v] obj]
                                                 (when-not (nil? v)
                                                   [:db/add -1 k v]))))]
    (tempids -1)))

(defmacro assert [exp]
  `(when-not ~exp
     (ut/throw (str "assertion failed: " ~(apply str (take 300 (pr-str exp)))))))

