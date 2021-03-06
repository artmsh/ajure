(ns ajure.handlers
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [expound.alpha :as expound]
            [clojure.spec.alpha :as s]
            [clojure.edn :as edn]))

(defn default-return-result [_ body _]
  {:success (get (json/parse-string body) "result")})

(defn get-boundary [content-type]
  (second (first (re-seq #"boundary=([0-9a-zA-Z\'()\/\+\,\-_]+)" content-type))))

(defn handle-response [handler status body headers output-spec]
  (cond
    (nil? handler) {:error {:type :unknown :message (str "Unknown error status " status)}}
    (fn? handler) (handler status body headers)
    (and (map? handler) (contains? handler :error)) handler
    (not (s/valid? output-spec body)) {:error {:type :malformed-output :message (expound/expound-str output-spec body)}}
    :else handler))

(defn get-status-headers-body [req lines]
  (let [status-line (nth lines 3)
        status (read-string (second (str/split status-line #" ")))
        [header-lines body-lines]
        (->> lines
          (drop 4)
          (split-with #(not= "" %)))

        body (second body-lines)
        headers (into {} (map #(str/split % #": " 2) header-lines))
        handler (get-in req [:responses status])]
    (handle-response handler status body headers (:output-spec req))))

(defn batch-parse-result [reqs _ body headers]
  (let [boundary (str "--" (get-boundary (get headers "content-type")))
        boundary-pattern (re-pattern boundary)
        splitted (->>
                   (str/split body boundary-pattern)
                   (drop-last)
                   (drop 1)
                   (map #(str/split % #"\r\n")))
        mapped (map get-status-headers-body reqs splitted)]
    {:success mapped}))

(defn body-json-success [_ body _] {:success (json/parse-string body keyword)})
(defn body-json-error [_ body-str _]
  (let [body (json/parse-string body-str)]
    {:error {:type (keyword (str "api-error-" (get body "errorNum")))
             :message (get body "errorMessage")}}))

(defn body-json-replication-success [status body headers]
  {:success       (if (str/blank? body)
                    []
                    (map #(json/parse-string % keyword) (str/split-lines body)))
   :check-more    (edn/read-string (get headers "x-arango-replication-checkmore"))
   :last-included (edn/read-string (get headers "x-arango-replication-lastincluded"))})

(defn not-found [message] {:error {:type :api-not-found :message message}})
(defn bad-request [message] {:error {:type :api-bad-request :message message}})
(defn forbidden [message] {:error {:type :api-forbidden :message message}})
(defn server-error [] {:error {:type :api-server-error :message "Internal server error"}})