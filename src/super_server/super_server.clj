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
            [super-server.user-accounts :as ua]))

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

(defn permanent-database! [db-schema]
  (let [config        {:store      {:backend :file
                                    :path    "ezbitmap_database"}}
        new-database? (not (dh/database-exists? config))]
    (when new-database?
      (dh/create-database config))
    (reset! db (dh/connect config))
    (dh/transact @db (datahike-schema db-schema))
    :ready))

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
                                                    (lr/resolve-as nil {:message (str (type e) ":" (ex-message e))}))))))))]
    (sn/modify {:objects   {nil {:fields {nil fix-resolver}}}
                :queries   {nil fix-resolver}
                :mutations {nil fix-resolver}}
               graphql-schema)))

(defn current-userid [context]
  (when-let [id (ua/session-user-id context)]
    (:userid (ua/user-by-id @db id))))

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
            (assoc-in context [:request :session] {:id "user:115"}))})

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
                            user-accounts?]
                     :as options}]
  (assert (empty? (se/difference #{:schema
                                   :local-react?
                                   :port
                                   :user-accounts?}
                                 (set (keys options)))))
  (assert @db)
  (let [session-interceptor (rm/session {:store (co/cookie-store)})
        schema              (cond-> schema
                              user-accounts? ua/attach-user-account-schema)
        schema              (sc/compile (fix-resolvers schema))
        servmap             (cond-> (lp/service-map schema
                                                    (when true 
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
                                                                                                      true         (inject session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context)
                                                                                                      local-react? (inject mock-session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context)
                                                                                                      true         (inject atomize-session-interceptor :com.walmartlabs.lacinia.pedestal/inject-app-context))))))))
                              true               (update :io.pedestal.http/routes concat (ro/expand-routes #{["/greet" :get `respond-greet]})))
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

