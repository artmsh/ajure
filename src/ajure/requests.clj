(ns ajure.requests
  (:require [ajure.protocols :as proto]
            [ajure.handlers :refer :all]))

(defrecord Request [http-method url protocol-method output-spec responses http-params])

(defn create-database
  ([db]       (->Request :post "/_api/database" #'proto/create-database map? {201 default-return-result} {:form-params {:name db} :content-type :json}))
  ([db users] (->Request :post "/_api/database" #'proto/create-database map? {201 default-return-result} {:form-params {:name db :users users} :content-type :json})))

(defn get-document
  ([handle]              (->Request :get (str "/_api/document/" handle) #'get-document map? {200 body-json-success 404 (not-found (str "Document '" handle "' not found"))} nil))
  ([handle rev strategy] (->Request :get (str "/_api/document/" handle) #'get-document map? {200 body-json-success 404 (not-found (str "Document '" handle "' not found"))} {:headers
                                                                                                                                                                             (case strategy
                                                                                                                                                                               :if-none-match {"If-None-Match" rev}
                                                                                                                                                                               :if-match {"If-Match" rev}
                                                                                                                                                                               {})})))

(defn exist-document?
  ([handle]              (->Request :head (str "/_api/document/" handle) #'exist-document? map? {200 {:success true} 404 {:success false}} nil))
  ([handle rev strategy] (->Request :head (str "/_api/document/" handle) #'exist-document? map? {200 {:success true} 404 {:success false}} {:headers
                                                                                                                                             (case strategy
                                                                                                                                               :if-none-match {"If-None-Match" rev}
                                                                                                                                               :if-match {"If-Match" rev}
                                                                                                                                               {})})))

(defn get-api-version []
  (->Request :get "/_api/version" #'get-api-version map? {200 body-json-success} nil))