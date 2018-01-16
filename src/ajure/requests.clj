(ns ajure.requests
  (:require [ajure.protocols :as proto]
            [ajure.handlers :refer :all]
            [cheshire.core :as json]
            [clojure.spec.alpha :as s]))

(defrecord Request [http-method url protocol-method output-spec responses http-params])

(defn create-database
  ([db]       (->Request :post "/_api/database" #'proto/create-database map? {201 default-return-result} {:form-params {:name db} :content-type :json}))
  ([db users] (->Request :post "/_api/database" #'proto/create-database map? {201 default-return-result} {:form-params {:name db :users users} :content-type :json})))

(defn get-document
  ([handle]              (->Request :get (str "/_api/document/" handle) #'proto/get-document map? {200 body-json-success 404 (not-found (str "Document '" handle "' not found"))} nil))
  ([handle rev strategy] (->Request :get (str "/_api/document/" handle) #'proto/get-document map? {200 body-json-success 404 (not-found (str "Document '" handle "' not found"))} {:headers
                                                                                                                                                                             (case strategy
                                                                                                                                                                               :if-none-match {"If-None-Match" rev}
                                                                                                                                                                               :if-match {"If-Match" rev}
                                                                                                                                                                               {})})))

(defn exist-document?
  ([handle]              (->Request :head (str "/_api/document/" handle) #'proto/exist-document? (s/or :s string? :n nil?) {200 {:success true} 404 {:success false}} nil))
  ([handle rev strategy] (->Request :head (str "/_api/document/" handle) #'proto/exist-document? (s/or :s string? :n nil?) {200 {:success true} 404 {:success false}} {:headers
                                                                                                                                             (case strategy
                                                                                                                                               :if-none-match {"If-None-Match" rev}
                                                                                                                                               :if-match {"If-Match" rev}
                                                                                                                                               {})})))

(defn get-api-version []
  (->Request :get "/_api/version" #'proto/get-api-version map? {200 body-json-success} nil))

(defn update-documents
  [collection documents]
  (->Request :patch (str "/_api/document/" collection) #'proto/update-documents map?
             {201 body-json-success
              202 body-json-success
              400 body-json-error
              404 body-json-error}
             {:body (json/generate-string documents)}))

(defn replace-document
  ([handle document replace-doc-options] (->Request :put (str "/_api/document/" handle) #'proto/replace-document map?
                                                    {201 body-json-success
                                                     202 body-json-success
                                                     400 body-json-error
                                                     404 body-json-error}
                                                    {:query-params replace-doc-options
                                                     :body (json/generate-string document)}
                                                    ))
  ([handle document rev replace-doc-options] (->Request :put (str "/_api/document/" handle) #'proto/replace-document map?
                                                        {201 body-json-success
                                                         202 body-json-success
                                                         400 body-json-error
                                                         404 body-json-error
                                                         412 body-json-error}
                                                        {:query-params replace-doc-options
                                                         :body (json/generate-string document)
                                                         :headers {"If-Match" rev}})))

(defn get-by-keys [collection keys]
  (->Request :put "/_api/simple/lookup-by-keys" #'proto/get-by-keys map?
             {200 (comp #(update % :success (fn [r] (get r "documents"))) body-json-success)
              404 body-json-error
              405 body-json-error}
             {:body (json/generate-string {:keys keys :collection collection})}))