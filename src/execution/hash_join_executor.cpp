//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/hash_join_executor.h"

#include <memory>
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left)),
      right_executor_(std::move(right)),
      jht_("hash join table", exec_ctx->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_) {}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
// Uncomment me! const HT *GetJHT() const { return &jht_; }

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  auto buffer_pool_manager = exec_ctx_->GetBufferPoolManager();
  page_id_t tmp_page_id;
  auto tmp_page = reinterpret_cast<TmpTuplePage *>(buffer_pool_manager->NewPage(&tmp_page_id)->GetData());
  tmp_page->Init(tmp_page_id, PAGE_SIZE);

  // builder hash table for left child
  Tuple tuple;
  TmpTuple tmp_tuple(tmp_page_id, 0);
  while (left_executor_->Next(&tuple)) {
    auto h_val = HashValues(&tuple, left_executor_->GetOutputSchema(), plan_->GetLeftKeys());
    // 插入 tuple 到 TmpTuplePage 失败，如果 Page 满了重新创建一个新的 Page
    if (!tmp_page->Insert(tuple, &tmp_tuple)) {
      buffer_pool_manager->UnpinPage(tmp_page_id, true);
      tmp_page = reinterpret_cast<TmpTuplePage *>(buffer_pool_manager->NewPage(&tmp_page_id)->GetData());
      tmp_page->Init(tmp_page_id, PAGE_SIZE);

      // 尝试重新插入元素到 TmpTuplePage
      tmp_page->Insert(tuple, &tmp_tuple);
    }
    // TmpTuple 插入到 HASH 表中
    jht_.Insert(exec_ctx_->GetTransaction(), h_val, tmp_tuple);
  }
  // Unpin TmpTuplePage 页，刷脏页到磁盘
  buffer_pool_manager->UnpinPage(tmp_page_id, true);
}

bool HashJoinExecutor::Next(Tuple *tuple) {
  auto buffer_pool_manager = exec_ctx_->GetBufferPoolManager();
  auto predicate = plan_->Predicate();
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto output_schema = GetOutputSchema();
  Tuple right_tuple;

  while (right_executor_->Next(&right_tuple)) {
    auto h_val = HashValues(&right_tuple, right_executor_->GetOutputSchema(), plan_->GetRightKeys());
    std::vector<TmpTuple> left_tmp_tuples;
    jht_.GetValue(exec_ctx_->GetTransaction(), h_val, &left_tmp_tuples);

    // 获取所有匹配的 left tuple
    for (auto &tmp_tuple: left_tmp_tuples) {
      // 转换 tmp tuple 为 left tuple
      auto page_id = tmp_tuple.GetPageId();
      auto tmp_page = buffer_pool_manager->FetchPage(page_id);
      Tuple left_tuple;
      left_tuple.DeserializeFrom(tmp_page->GetData() + tmp_tuple.GetOffset());
      buffer_pool_manager->UnpinPage(page_id, false);

      // 创建 output tuple
      if (!predicate || predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < output_schema->GetColumnCount(); ++i) {
          auto expr = output_schema->GetColumn(i).GetExpr();
          values.push_back(expr->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
        }
        *tuple = Tuple(values, output_schema);
        return true;
      }
    }
  }
  return false;
}
}  // namespace bustub
