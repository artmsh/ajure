(ns ajure.load-test
  (:require [clojure.test :refer :all]
            [ajure.requests :as reqs]
            [clojure.string :as str]
            [ajure.protocols :refer :all]
            [clojure.string :as string])
  (:import (ajure.core ArangodbApi)))

;(def api (ArangodbApi. "http://10.100.0.1:8529"))
(def api (ArangodbApi. "http://localhost:8529"))

; Time 1000 OR clauses vs DOCUMENT vs batch vs lookup-by-keys
(deftest test-get-by-key-1000-objects
  (testing "Time 1000 OR clauses vs DOCUMENT vs batch vs lookup-by-keys"
    (let [ids (:success (create-cursor api "tom"
                                       {:query "FOR b in body return b.id"
                                        :args {}}
                                       {}))
          clause (clojure.string/join " OR " (map #(str "b.id == '" % "'") ids))
          time-1 (time (create-cursor api "tom"
                                      {:query (str "FOR b in body filter " clause " return b")
                                       :args {}}
                                      {}))
          time-2 (time (create-cursor api "tom"
                                      {:query (str "RETURN DOCUMENT(body, [" (str/join "," (map #(str "\"" % "\"") ids)) "])")
                                       :args  {}}
                                      {}))
          reqs (map #(reqs/get-document (str "body/" %)) ids)
          time-3 (time (batch api "tom" reqs))
          time-4 (time (get-by-keys api "tom" "body" ids))]
      (prn (ffirst time-1))
      (prn (ffirst time-2))
      (prn (ffirst time-3))
      (prn (ffirst time-4))
      (when (= (ffirst time-2) :error)
        (prn time-2)))))

(defn- build-clauses [ids field op]
  (string/join (str " " op "  ") (map #(str field " == \"" % "\"") ids)))

(defn- build-body-id-filter-clauses [body-ids]
  (build-clauses body-ids "a.body_id" "OR"))

(deftest test-get-by-field-1000-objects
  (let [ids (take 1000 (-> (get-all-document-keys api "tom" "body" :key)
                           :success
                           :result))
        ids-json (str "['" (str/join "','" ids) "']")
        ids-filter-clause (build-body-id-filter-clauses ids)
        time-1 (time (create-cursor api "tom"
                                    {:query (str "for a in attributes filter a.body_id in " ids-json " return a") :args {}}
                                    {}))
        time-2 (time (create-cursor api "tom"
                                    {:query (str "for a in attributes "
                                                 "filter " ids-filter-clause " "
                                                 "return a")
                                     :args {}}
                                    {}))]
    (prn (count (-> time-1 :success :result)))
    (prn (count (-> time-2 :success :result)))))