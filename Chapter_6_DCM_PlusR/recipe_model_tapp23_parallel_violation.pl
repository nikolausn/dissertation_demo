#include "recipe_model_tapp23_analysis.pl".


column_schema(cs10,name,"Title"). %column_schema(cs10,data_type,text).

process(p11,column_rename).
%process_input(p11,1,cs2).
%process_output(p11,1,cs10).

process_in_out(p11,cs2,cs10).
process_in_out(p11,cs2,csRemoved).


column_schema(cs11,name,"New Title"). %column_schema(cs11,data_type,text).
process(p12,create_new_column).
%process_input(p11,1,cs2).
%process_output(p11,1,cs10).

process_in_out(p12,csNone,cs11).

recipe(r1_parallel,1,p1,0).
recipe(r1_parallel,2,p2,0).
recipe(r1_parallel,7,p7,0).
recipe(r1_parallel,3,p3,2).
recipe(r1_parallel,8,p8,7).
recipe(r1_parallel,4,p4,3).
recipe(r1_parallel,5,p5,4).
recipe(r1_parallel,6,p6,4).
recipe(r1_parallel,9,p9,1).
recipe(r1_parallel,9,p9,8).
recipe(r1_parallel,9,p9,5).
%recipe(r1_parallel,11,p12,10).

recipe(r1_serial_violation,1,p1,0).
recipe(r1_serial_violation,b1,p11,1).
%recipe(r1_serial_violation,2,p2,1).
recipe(r1_serial_violation,2,p2,b1).
recipe(r1_serial_violation,3,p3,2).
recipe(r1_serial_violation,4,p4,3).
recipe(r1_serial_violation,5,p5,4).
recipe(r1_serial_violation,6,p6,5).
recipe(r1_serial_violation,7,p7,6).
recipe(r1_serial_violation,8,p8,7).
recipe(r1_serial_violation,9,p9,8).

% serial_violation on paralell sense
recipe(r1_parallel_violation_1,1,p1,0).
%example for update parallel violation 
%recipe(r1_parallel_violation_1,b1,p11,1).
recipe(r1_parallel_violation_1,2,p2,0).
recipe(r1_parallel_violation_1,3,p3,2).
recipe(r1_parallel_violation_1,4,p4,3).
recipe(r1_parallel_violation_1,5,p5,4).
recipe(r1_parallel_violation_1,6,p6,4).
recipe(r1_parallel_violation_1,7,p7,0).
recipe(r1_parallel_violation_1,8,p8,7).
%recipe(r1_parallel_violation_1,9,p9,1).
recipe(r1_parallel_violation_1,9,p9,8).
recipe(r1_parallel_violation_1,9,p9,5).
%recipe(r1_parallel_violation_1,10,p10,1).
%example for update parallel violation 
recipe(r1_parallel_violation_1,b1,p11,1).
recipe(r1_parallel_violation_1,9,p9,b1).

% read violation two process reading a same input schema
recipe(r1_parallel_violation_2,1,p1,0).
recipe(r1_parallel_violation_2,2,p2,0).
recipe(r1_parallel_violation_2,3,p3,2).
recipe(r1_parallel_violation_2,4,p4,3).
recipe(r1_parallel_violation_2,5,p5,4).
%recipe(r1_parallel_violation_2,6,p6,4).
recipe(r1_parallel_violation_2,6,p13,4).
recipe(r1_parallel_violation_2,b6,p1,6).
recipe(r1_parallel_violation_2,7,p7,0).
recipe(r1_parallel_violation_2,8,p8,7).
recipe(r1_parallel_violation_2,9,p9,1).
recipe(r1_parallel_violation_2,9,p9,8).
recipe(r1_parallel_violation_2,9,p9,5).

% write violation two process reading a same input schema
recipe(r1_parallel_violation_3,1,p1,0).
recipe(r1_parallel_violation_3,2,p2,0).
recipe(r1_parallel_violation_3,3,p3,2).
recipe(r1_parallel_violation_3,4,p4,3).
recipe(r1_parallel_violation_3,5,p5,4).
%recipe(r1_parallel_violation_3,6,p6,4).
recipe(r1_parallel_violation_3,6,p13,4).
recipe(r1_parallel_violation_3,7,p7,0).
recipe(r1_parallel_violation_3,8,p8,7).
recipe(r1_parallel_violation_3,9,p9,1).
recipe(r1_parallel_violation_3,9,p9,8).
recipe(r1_parallel_violation_3,9,p9,5).



process(p13,column_rename).
%process_input(p11,1,cs2).
%process_output(p11,1,cs10).

process_in_out(p13,cs7,cs3).
process_in_out(p13,cs7,csRemoved).


recipe(r1_parallel_violation_4,1,p1,0).
recipe(r1_parallel_violation_4,2,p2,0).
recipe(r1_parallel_violation_4,3,p3,2).
recipe(r1_parallel_violation_4,4,p4,3).
recipe(r1_parallel_violation_4,5,p5,4).
recipe(r1_parallel_violation_4,6,p6,4).
recipe(r1_parallel_violation_4,7,p13,0).
recipe(r1_parallel_violation_4,8,p8,7).
recipe(r1_parallel_violation_4,9,p9,1).
recipe(r1_parallel_violation_4,9,p9,8).
% remove required workflow linkage (parallel recipe)
% recipe(r1_parallel_violation_4,9,p9,5).


% any process on a workflow walk that does not have required input schema
parallel_recipe_violation(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,PidM,Min,Mout,CSin,Sid) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,Min,Mout,PidM,Sid),
    process_input(PidM,_,CSin),
    not derived_output_inv_clean(Rid,_,_,_,_,_,N,_,schema_state(CSin,_),_,_,_).

% any process on the same execution level requires the same input schema 
parallel_recipe_violation(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,PidM,schema_state(CSinA,SSA),Mout,CSinA,SidA) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,schema_state(CSinA,SSA),Mout,PidM,SidA),
    derived_output_inv_clean(Rid,_,_,_,_,_,_,_,schema_state(CSinA,SSA),_,_,SidB),
    SidA!=SidB.

% any process where it has multiple schemas input at particular level
parallel_recipe_violation(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,PidM,schema_state(CSinA,SSA),Mout,CSinA,SidA) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,schema_state(CSinA,SSA),Mout,PidM,SidA),
    H=#count{SignX:derived_output_inv_clean(Rid,_,_,_,SignX,_,N,_,schema_state(CSinA,_),_,_,_)},
    H>1.

% any process from different walk (path signature) on the same rank that produce same output schema
parallel_recipe_violation(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,PidM,schema_state(CSinA,SSA),schema_state(CSoutA,SSB),CSinA,SidA) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,schema_state(CSinA,SSA),schema_state(CSoutA,SSB),PidM,SidA),
    derived_output_inv_clean(Rid,_,_,_,_,SignB,_,_,_,schema_state(CSoutA,_),_,SidB),
    CSoutA!=csRemoved,
    SidA!=SidB,
    Sign!=SignB.

    
#show parallel_recipe_violation/12.

%parallel_recipe_violation(Rid,A1,B1,A2,B2,S1,S2,S3,S4,Sign1,Sign2,seq(SS2,PP1),seq(SS4,PP2),CS1) :-
%    test_serialize_recipe_unique(Rid,A1,B1,S1,S2,seq(SS1,SS2),M1,Sign1,_),
%    test_serialize_recipe_unique(Rid,A2,B2,S3,S4,seq(SS3,SS4),M2,Sign2,_),
%    Sign1!=Sign2,
%    recipe(Rid,SS2,PP1,_),
%    recipe(Rid,SS4,PP2,_),
%    process_input(PP1,_,CS1),
%    process_output(PP2,_,CS1),
%    SS2>SS4,
%    not .
%#show parallel_recipe_violation/14.

derived_output_recipe(Rid,N,Mout,PidM) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,Min,Mout,PidM,Sid).

derived_output_recipe(Rid,N,schema_state(CSin,SSin),PidM) :-
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,_,schema_state(CSin,SSin),schema_state(CSout,_),PidM,Sid),
    CSout!=csRemoved,
    CSout!=CSin.

derived_output_recipe(Rid,N+1,schema_state(CSin,SSin),PidM) :-
    derived_output_recipe(Rid,N+1,schema_state(CSin,SSin),PidM),
    not derived_output_recipe(Rid,N,schema_state(CSin,_),_).

derived_output_recipe_2(Rid,N,schema_state(CSin,SSout),PidM) :-
    derived_output_inv_clean(Rid,_,_,_,_,_,N,_,_,_,_,_),
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,MN,_,schema_state(CSin,SSin),schema_state(CSin,SSout),PidM,Sid),
    max_derived_output_inv_n(Rid,NN),   
    MN=N+1..NN.

derived_output_recipe_1(Rid,N,schema_state(CSout,SSout),PidM) :-
    derived_output_inv_clean(Rid,_,_,_,_,_,N,_,_,_,_,_),
    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,MN,_,schema_state(CSin,SSin),schema_state(CSout,SSout),PidM,Sid),
    not derived_output_recipe(Rid,N,schema_state(CSout,_),_),
    max_derived_output_inv_n(Rid,NN),   
    MN=N+1..NN.

%derived_output_recipe_not(Rid,N,schema_state(CSin,SSin),schema_state(CSout,SSout),PidM) :-
%    derived_output_inv_clean(Rid,_,_,_,_,_,N,_,schema_state(CSout,SSout),_,_,_),
%    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,MN,_,schema_state(CSin,SSin),schema_state(CSout,SSout),PidM,Sid),
%    max_n(Rid,NN),   
%    MN=N+1..NN.%
%derived_output_recipe(Rid,N,Min,Mout,PidM) :-
%    derived_output_inv_clean(Rid,_,_,_,_,_,N,_,_,_,_,_),
%    derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,MN,_,Min,Mout,PidM,Sid),
%    max_n(Rid,NN),   
%    MN=N+1..NN,
%    not derived_output_recipe_not(Rid,N,Min,Mout,PidM).
