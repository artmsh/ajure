(ns ajure.core-test
  (:require [clojure.test :refer :all]
            [ajure.core :refer :all]
            [ajure.protocols :refer :all]
            [ajure.pull :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.data]
            [expound.alpha :as expound]
            [ajure.requests :as reqs]
            [clojure.string :as str])
  (:import (ajure.core ArangodbApi)
           (ajure.pull ArangodbPullApi)))

(def api (ArangodbApi. "http://localhost:8529"))
(def pull-api (ArangodbPullApi. api))


(defn is-eq-map? [expected actual-full]
  (let [call-result (:success actual-full)]
    (if (nil? call-result)
      false
      (= expected (select-keys call-result (keys expected))))))


(deftest db-test
  (testing "Getting current database"
    (let [res (get-current-database api)]
      (is (= "_system" (get-in res [:success "name"])))))

  (testing "Full integration"
    (let [dbs-before (get-all-databases api)
          cd-testdb (create-database api "testdb")
          cc-test-coll (create-collection api "testdb" {:name "test_collection"})
          gc-testdb (get-collections api "testdb")
          cd-0 (create-document api "testdb" "test_collection" [{:_key "0" :a 1 :b 2}] {})
          gc-test-coll (get-collection api "testdb" "test_collection")
          ci-test-index (-> (create-index api "testdb" "test_collection" {:type "hash"
                                                                          :fields ["a"]
                                                                          :unique false
                                                                          :sparse false})
                            :success
                            (dissoc :id))
          ed-0 (exist-document? api "testdb" "test_collection/0")
          ed-1_ (exist-document? api "testdb" "test_collection/1")
          id-1-0 (import-documents api "testdb" [{:_key "1" :a 2 :c -1} {:_key "0" :a 3}]
                                   {:collection "test_collection" :type "list" :onDuplicate "update"})
          gcp-test-coll (get-collection-properties api "testdb" "test_collection")
          ed-1 (exist-document? api "testdb" "test_collection/1")
          gd-0 (get-document api "testdb" "test_collection/0")
          uds-0 (update-documents api "testdb" "test_collection" [{:_key "0" :a 2}] {})
          rd-0 (remove-document api "testdb" "test_collection/0" {})
          rc-test-coll (remove-collection api "testdb" "test_collection" {})
          rd-testdb (remove-database api "testdb")
          dbs-after (get-all-databases api)]

      (is (= {:success true} cd-testdb))
      (is (is-eq-map?
            {:name "test_collection" :waitForSync false :isSystem false :isVolatile false
             :status 3 :type 2}
            cc-test-coll))
      (is (is-eq-map?
            {"name" "test_collection" "isSystem" false "status" 3 "type" 2}
            {:success (first (filter #(= (get % "isSystem") false) (:success gc-testdb)))}))
      (is (is-eq-map?
            {:_id "test_collection/0" :_key "0"}
            {:success (first (:success cd-0))}))
      (is (is-eq-map?
            {:name "test_collection" :isSystem false :status 3 :type 2}
            gc-test-coll))
      (is (= {:success true} ed-0))
      (is (= {:success false} ed-1_))
      (is (is-eq-map?
            {:error false :created 1 :errors 0 :empty 0 :updated 1 :ignored 0}
            id-1-0))
      (is (is-eq-map?
            {:name "test_collection"
             :isSystem false
             :doCompact true
             :isVolatile false
             :keyOptions { :type "traditional" :allowUserKeys true :lastValue 1}
             :waitForSync false
             :status 3
             :type 2}
            gcp-test-coll))
      (is (= {:success true} ed-1))
      (is (is-eq-map?
            {:_id "test_collection/0" :_key "0" :a 3 :b 2}
            gd-0))
      (is (= {:success (list {:_id "test_collection/0" :_key "0"})}
             (update uds-0 :success #(map (fn [d] (select-keys d #{:_id :_key})) %))))
      (is (is-eq-map?
            {:_id "test_collection/0" :_key "0"}
            rd-0))
      (is (is-eq-map?
            {:id (get-in cc-test-coll [:success :id]) :error false}
            rc-test-coll))
      (is (= true (:success rd-testdb)))
      (is (= dbs-before dbs-after))
      (is (= {:unique false
              :selectivityEstimate 1
              :isNewlyCreated true
              :fields ["a"]
              :type "hash"
              :code 201
              :deduplicate true
              :error false
              :sparse false}
             ci-test-index)
          "Index creation functions as expected")))

  (testing "AQL Queries"
    (prn (create-cursor api "tom"
                        {:query "FOR l in location return l"
                         :args {}}
                        {}))
    (prn (create-cursor api "tom"
                        {:query "RETURN VALUES(KEEP(DOCUMENT('location/t1p'), ['id', 'locationType']))"
                         :args {}}
                        {}))

    (loop [res 0
           q (:success (create-cursor api "tom" {:query "FOR b in body return b.id"
                                                 :args {}}
                                      {}))]
      (prn "res: " res "cursor:" (:id q))
      (if (:hasMore q)
        (recur (+ res (count (:result q))) (:success (get-next-cursor-batch api "tom" (:id q))))
        (+ res (count (:result q)))))

    (let [res (create-cursor api "tom" {:query "FOR l in location return l"
                                        :args {}}
                             {:batchSize 2})
          _ (prn res)
          cursor-id (get-in res [:success :id])]
      (get-next-cursor-batch api "tom" cursor-id))

    (batch api "tom"
           [(reqs/get-replication-logger-state)
            (reqs/get-document "location/sedanos-32")])) ;["sedanos-32"])]))
            ;(reqs/get-all-documents "accounts")]))

  (testing "Pull API"
    #_(pull pull-api '[:body/id :body/barcode :attributes/_body_id
                       {:body/descriptions [:description/id :description/value]}] ["tom" "body/00073480000341"])
    (pull pull-api '[:body/id :body/scalable :attributes/_body_id] ["tom" "body/00050000836123"])
    #_(pull-many pull-api '[{:body/taxa [:taxon/taxon_id]}] (map #(vector "tom" (str "body/" %))
                                                                 #{"00050000836123" "00720473231588"})))
  #_
  (testing "Batch API"
    (get-document api "tom" "location/t1p")
    (get-collections api "tom")
    (prn (batch api "tom" [(reqs/get-by-example "attributes" {:body_id "00050000836123"} {})
                           (reqs/get-by-example "attributes" {:body_id "00720473231588"} {})]))

    (batch api "tom" [(reqs/exist-document? "assets/2901034002900")])

    (let [assets-data [{:id "2901034002900", :type :tote, :body_id "00720473231588"}]
          reqs (mapv #(reqs/exist-document? (str "assets/" (:id %))) assets-data)
          _ (prn "reqs:" reqs)
          res (batch api "tom" reqs)]
      res)

    (prn (batch api "tom" [(reqs/get-api-version)
                           (reqs/get-document "location/t1p")]))
    (is (= (batch api "tom" [(reqs/exist-document? "location/t1p")
                             (reqs/exist-document? "location/never-ever-exist")])
           {:success [{:success false} {:success false}]}))
    (let [tid "taxon/d8f083e50aadc403980ab161da13f2789fe30990b52b23df14b9bf2f699b6578"
          aid "attributes/bfresh-allston:bfresh00208887000001"]
      (batch api "tom" [(reqs/exist-document? aid)
                        (reqs/get-document aid)
                        (reqs/exist-document? tid)
                        (reqs/get-document tid)])))

  #_
  (testing "Simple API"
    (get-collection-count api "tom" "taxon")
    (get-all-document-keys api "tom" "location" :key)
    (loop [res 0
           q (:success (get-all-documents api "tom" "body"))]
      (prn "res: " res "cursor:" (:id q))
      (if (:hasMore q)
        (recur (+ res (count (:result q))) (:success (get-next-cursor-batch api "tom" (:id q))))
        (+ res (count (:result q)))))

    (get-first-example api "tom" "tree_props" {:tree_id "bfresh"})

    (prn (get-by-keys api "tom" "location" ["bfresh-allston"]))
    (let [extr (fn [r] (->> r :success :result (map :_id)))]
      [(extr (get-by-example api "tom" "products" {:location/id "sedanos-32"} {:batchSize 10}))
       (extr (get-by-example api "tom" "products" {:location/id "sedanos-32"} {:limit 10}))])

    (update-documents api "tom" "location" [{:_key "id-1" :locationType "SPOKE"} {:_key "sally" :a 2}] {})
    (update-document api "tom" "workers/availability-worker"
      {:workers/couch-last-seqs
       {"o" "1166805-g1AAAAI7eJzLYWBg4MhgTmHgzcvPy09JdcjLz8gvLskBCjMlMiTJ____PyuDOYmBNe93LlCM3czUMtHEyABdPQ4TkhSAZJI9zBAWhu9gQ9ISjS2SDE2JNcQBZEg8zBAGzv9gQwzNLU1SjIk2JAFkSD3ckO8yYEMszSzMUtKSiTQkjwVIMjQAKaA588EGMe9sBRtkYWaWlJRoQZJBCyAG7QcbxCT5GBLAlikpBqbEBjDEoAMQg-5DvHbkBNig5DRzs0TDRJIMegAxCBpGR2aADTI1NTayMDVH15YFAJTIsMI"}}
      {})
    (get-all-documents api "tom" "location" 3)
    (get-first-example api "tom" "location" {:id "bfresh-allston"}))

  (testing "Replication"
    (loop [{:keys [success check-more last-included]} (get-replication-dump api "tom" "settings" {})
           coll {}]
      (if-not check-more
        coll
        (recur (get-replication-dump api "tom" "settings" {:from last-included})
               (reduce
                 (fn [c {:keys [type data]}]
                   (prn (format "Operation type:[%d], key: %s" type (:_key data)))
                   (cond
                     (= type 2300)
                     (do
                       (when (contains? c (:_key data))
                           (prn (format "Update doc[%s], delta: %s" (:_key data)
                                        (second (clojure.data/diff (get c (:_key data)) data)))))
                       (assoc c (:_key data) data))
                     (= type 2302) (dissoc c (:_key data))))
                 coll
                 success))))
    (prn (get-replication-dump api "tom" "settings" {}))))

(defn create-assets [assets-data]
  (let [db "tom"
        _ (prn assets-data)
        reqs (mapv #(reqs/exist-document? (str "assets/" (:id %))) assets-data)
        _ (prn "reqs:" reqs)
        res (batch api db reqs)]
    res))