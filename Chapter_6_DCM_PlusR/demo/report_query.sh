facts=recipe_model_facts.pl
queries=recipe_model_analysis.pl
parallel=recipe_model_parallel_inference.pl
parallel_violation=recipe_model_parallel_violation.pl
execution_violation=recipe_model_execution_violation.pl

temp_view=temp_view.pl

echo "Q1. What are the minimal input schemas needed to execute a data-cleaning recipe?"
echo "#show minimal_input_schema_recipe/4." > $temp_view
clingo $queries $temp_view
echo ""
echo "Q2. What are the expected output schemas produced after executing a data-cleaning recipe?"
echo "#show derived_output_schema_recipe/4." > $temp_view
clingo $queries $temp_view
echo ""
echo "Q3. Based on the required schema, what are processes dependent on each other such that they can be executed in parallel?"
echo "#show parallel_recipe/5." > $temp_view
clingo $parallel $temp_view
echo ""
echo "Q4. When hypothetically updating/wireframing processes on an existing recipe, is the new recipe well-formed and runnable?"
echo "Q4.(a). Serial violation"
echo "
parallel_recipe_violation_r1_serial(Rid,S1,S2,S3,S4,S5,S6,S7,S8,S9,S10,S11) :-
    parallel_recipe_violation(Rid,S1,S2,S3,S4,S5,S6,S7,S8,S9,S10,S11),
    Rid=r1_serial_violation.
#show parallel_recipe_violation_r1_serial/12." > $temp_view
echo "Q4.(b). Parallel violation"
echo "
parallel_recipe_violation_r1_parallel(Rid,S1,S2,S3,S4,S5,S6,S7,S8,S9,S10,S11) :-
    parallel_recipe_violation(Rid,S1,S2,S3,S4,S5,S6,S7,S8,S9,S10,S11),
    Rid=r1_parallel_violation_1.
#show parallel_recipe_violation_r1_parallel/12." > $temp_view
clingo $parallel $temp_view
echo ""
echo "Q5. Given a known dataset Ds_{new} with schema S_{new}, is a data-cleaning recipe r1 can be reused on the Ds_{new}?"
echo "#show minimal_input_schema_recipe/4. #show execution_violation/6." > $temp_view
clingo  $execution_violation $temp_view
