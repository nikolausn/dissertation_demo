facts=collaboative_model_facts.pl
queries=collaborative_model_values_reporting.pl
process_queries=collaborative_model_process_analysis.pl
temp_view=temp_view.pl

echo "Q1. Who is changing the dataset and what are the changes?"
echo "#show user_changes/7." > $temp_view
clingo $queries $temp_viewcli
echo ""
echo "Q2. How different are the resulting dataset within different users' data-cleaning scenarios?"
echo "#show snapshot_value/5." > $temp_view
clingo $queries $temp_view
echo ""
echo "Q3. Are there any conflicting changes between different snapshots? (Example for snapshot at state 3 and 4)?"
echo "#show snapshot_value_3_4/6." > $temp_view
clingo $queries $temp_view
echo ""
echo "Q4. How do we merge changes for non-conflicting cells? (Example combining state 2, 3 and 4)"
echo "#show merge_snapshot/4." > $temp_view
clingo $queries $temp_view
echo ""
echo "Q5. What steps are conflicting in different data-cleaning scenarios? (Example for workflow path 0 -> 1)"
echo "#show read_violation_0_1/13." > $temp_view
clingo $process_queries $temp_view
echo ""
echo "Q6. In case of evolving or updating workflow, which users are contributing to the new workflow?"
echo "#show attribution/5." > $temp_view
clingo $queries $temp_view

