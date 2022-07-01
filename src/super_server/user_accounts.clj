(ns super-server.user-accounts
  (:require [datahike.api :as dh]
            [fbc-utils.core :as ut]
            [buddy.hashers :as hs]))

(def salt "eldkgfadsflkasdj")

#_(defn validate-hash []
    (hs/derive "foo" {:salt salt}))

(defn make-hash [s]
  (hs/derive s {:salt salt}))

(defn numerical-id [id] ;all outward-facing ids have a uniquifying prefix attached to the id. All internal ids are numerical
  (if-let [[_ id] (re-find #"^.+\:([0-9]+)$" id)]
    (Integer. id)
    (ut/throw (str "bad uniquified id " id))))

(defn user-by-userid [db userid]
  (when-let [[[id password]] (seq (dh/q '[:find ?id ?password
                                          :in $ ?userid
                                          :where [?id :user/userid ?userid]
                                          [?id :user/password ?password]]
                                        @db
                                        userid))]
    {:id       id
     :password password}))

;;(user-by-userid @ss/db "drcode")

(defn user-by-id [db id]
  (when-let [[[userid]] (seq (dh/q '[:find ?userid
                                     :in $ ?id
                                     :where [?id :user/userid ?userid]]
                                   @db
                                   id))]
    {:id       (str "user:" id)
     :userid   userid}))

(defn session-user-id [{:keys [request]
                        :as   context}]
  (when-let [id (:id @(:session-response request))]
    (numerical-id id)))

(defn app-user [db context args app]
  (when-let [id (session-user-id context)]
    (user-by-id db id)))

(defn mutation-register-user [db
                              context
                              {:keys [userid
                                      password]
                               :as   args}
                              value]
  (cond (< (count userid) 3) (ut/throw "Userid must be at least 3 characters long")
        (< (count password) 5) (ut/throw "Password must be at least 5 characters long")
        :else (if (user-by-userid db userid)
                (ut/throw "Userid already taken")
                (let [{:keys [tempids
                              db-after]} (dh/transact db [[:db/add -1 :user/userid userid]
                                                          [:db/add -1 :user/password (make-hash password)]])]))))

(defn mutation-login-user [db
                           {:keys [request]
                            :as   context}
                           {:keys [userid
                                   password]
                            :as   args}
                           value]
  (if-let [password-provided password]
    (let [{:keys [session-response]} request
          {:keys [id
                  password]}         (user-by-userid db userid)]
      (if (= password (make-hash password-provided))
        (do (reset! session-response {:id (str "user:" id)})
            {:id "ezbitmap:0"})
        (ut/throw "Login failed")))
    (ut/throw "Login failed")))

(defn mutation-logout-user [db
                            {:keys [request]
                             :as   context}
                            args
                            value]
  (let [{:keys [session-response]} request]
    (reset! session-response nil)
    {:id "ezbitmap:0"}))

(def user-account-schema {:objects   {:App      {:fields {:user      {:type :User
                                                                      :resolve #'app-user}}}
                                      :User     {:fields {:id       {:type '(non-null ID)}
                                                          :userid   {:type '(non-null String)}}}}
                          :queries   {:app {:type    :App
                                            :args    {}}}
                          :mutations {:registerUser {:type    :User
                                                     :args    {:userid   {:type '(non-null String)}
                                                               :password {:type '(non-null String)}}
                                                     :resolve #'mutation-register-user}
                                      :loginUser    {:type    :App
                                                     :args    {:userid   {:type '(non-null String)}
                                                               :password {:type '(non-null String)}}
                                                     :resolve #'mutation-login-user}
                                      :logoutUser   {:type    :App
                                                     :args    {}
                                                     :resolve #'mutation-logout-user}}})

(defn deep-merge [& maps]
  (letfn [(reconcile-keys [val-in-result val-in-latter]
            (if (and (map? val-in-result)
                     (map? val-in-latter))
              (merge-with reconcile-keys val-in-result val-in-latter)
              val-in-latter))
          (reconcile-maps [result latter]
            (merge-with reconcile-keys result latter))]
    (reduce reconcile-maps maps)))

(defn attach-user-account-schema [schema]
  (deep-merge schema user-account-schema))
