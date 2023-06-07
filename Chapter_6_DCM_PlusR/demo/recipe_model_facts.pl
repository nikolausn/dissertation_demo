% column_schema(id,name,data_type).
% a column is presented as "name", with "date_type"
column_schema(cs1,name,"Book Title"). %column_schema(cs1,data_type,text).
column_schema(cs2,name,"TItle"). %column_schema(cs2,data_type,text).
column_schema(cs3,name,"Date"). %column_schema(cs3,data_type,text).
%column_schema(cs4,name,"Date"). %column_schema(cs4,data_type,number).
column_schema(cs5,name,"Author").%column_schema(cs5,data_type,text).
column_schema(cs6,name,"Author 1"). %column_schema(cs6,data_type,text).
column_schema(cs7,name,"Author 2"). %column_schema(cs7,data_type,text).
column_schema(cs8,name,"Last Name"). %column_schema(cs8,data_type,text).
column_schema(cs9,name,"Bib Reference"). %column_schema(cs9,data_type,text).


% standard schema data type only
%column_schema(csText,data_type,text).
%column_schema(csNumber,data_type,number).
%column_schema(csDate,data_type,date).

% standard process
%process(psMassEdit,mass_edit).
process_input(psMassEdit,csText).
process_output(psMassEdit,csText).

%process(psToNumber,to_number).
process_input(psToNumber,csText).
process_input(psToNumber,csNumber).

% process(id,name).
% a process with `id` has name `name`
process(p1,column_rename).
% input parameter of process blueprint
% entity of input_parameter for a process pid where parameter_name has parameter_value 
%process_input(p1,1,cs1).
% remark these output for violation example
% removing the original column cs1 which used by one of the process af
%process_output(p1,1,cs2).

process_in_out(p1,cs1,cs2).
process_in_out(p1,cs1,csRemoved).


%process(p2,column_move).
%process_input(p2,1,cs3).
%process_output(p2,1,cs3).

%process_in_out(p2,cs3,cs3).

process(p2,edit_author_fixed_last_name).
%process_input(p3,1,cs5).
%process_output(p3,1,cs5).

process_in_out(p2,cs5,cs5).

process(p3,edit_author_flag_missing).
%process_input(p4,1,cs5).
%process_output(p4,1,cs5).

process_in_out(p3,cs5,cs5).

process(p4,author_split).
%process_input(p5,1,cs5).
%process_output(p5,1,cs5).
%process_output(p5,1,cs6).
%process_output(p5,1,cs7).

process_in_out(p4,cs5,cs6).
process_in_out(p4,cs5,cs7).


process(p5,rename_author_last_name).
%process_input(p6,1,cs6).
%process_output(p6,1,cs8).

process_in_out(p5,cs6,cs8).
process_in_out(p5,cs6,csRemoved).


process(p6,remove_author_first_name).
%process_input(p7,1,cs7).

process_in_out(p6,cs7,csRemoved).

process(p7,fixed_year).
%process_input(p8,1,cs3).
%process_output(p8,1,cs3).

process_in_out(p7,cs3,cs3).

process(p8,trim_year).
%process_input(p9,1,cs3).
%process_output(p9,1,cs3).

process_in_out(p8,cs3,cs3).

process(p9,combined_bib).
%process_input(p10,1,cs2).
%process_input(p10,1,cs3).
%process_input(p10,1,cs8).
%process_output(p10,1,cs2).
%process_output(p10,1,cs3).
%process_output(p10,1,cs8).
%process_output(p10,1,cs9).

process_in_out(p9,cs2,cs9).
process_in_out(p9,cs3,cs9).
process_in_out(p9,cs8,cs9).

process_input(Pid,1,Input) :-
    process_in_out(Pid,Input,_).

process_output(Pid,1,Input) :-
    process_in_out(Pid,Input,Output),
    not process_in_out(Pid,Input,csRemoved).

process_output(Pid,1,Output) :-
    process_in_out(Pid,Input,Output),
    Output!=csRemoved.

derived_from(p2,psMassEdit).
derived_from(p3,psToNumber).

% recipe(recipe_id,seq,process_id,prev_seq_id).
% recipe for `recipe_id` has a process of `process_id` 
% at seq `seq` executes after `prev_seq_id`

% in recipe `recipe_id` process `process_id` is executed at step `seq_id`, with predecessor `prev_seq_id`


% for a recipe r1 there is a seq 1 which has process p1 executes after seq 0 
% in recipe `r1` process `p1` is executed at step `1`, with predecessor `0`

% sequential
recipe(r1,1,p1,0).
recipe(r1,2,p2,1).
recipe(r1,3,p3,2).
recipe(r1,4,p4,3).
recipe(r1,5,p5,4).
recipe(r1,6,p6,5).
recipe(r1,7,p7,6).
recipe(r1,8,p8,7).
recipe(r1,9,p9,8).
%recipe(r1,10,p10,9).


% random parallel
%recipe(r2,1,p1,0).
%recipe(r2,2,p2,1).
%recipe(r2,3,p3,2).
%recipe(r2,4,p4,2).
%recipe(r2,5,p5,4).
%recipe(r2,6,p6,3).
%recipe(r2,7,p7,6).
%recipe(r2,8,p8,7).
%recipe(r2,9,p9,8).

%0 - 1 - 2 - 3 - 6 - 7 - 8 - 9
%            4 - 5   

%#show process_input/3.
%#show process_output/3.