(ns ajure.handlers
  (:require [cheshire.core :as json]))

(defn default-return-result [_ body]
  {:success (get (json/parse-string body) "result")})

(defn batch-parse-result [_ body]
  {:success body})

(defn body-json-success [_ body] {:success (json/parse-string body)})
(defn body-json-error [_ body-str]
  (let [body (json/parse-string body-str)]
    {:error {:type (keyword (str "api-error-" (get body "errorNum")))
             :message (get body "errorMessage")}}))

(defn not-found [message] {:error {:type :api-not-found :message message}})
(defn bad-request [message] {:error {:type :api-bad-request :message message}})
(defn forbidden [message] {:error {:type :api-forbidden :message message}})
(defn server-error [] {:error {:type :api-server-error :message "Internal server error"}})