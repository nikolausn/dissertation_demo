% source(source_id, file_name, description)
source(0,"employee_demo","OpenRefine Project").
% array(array_id, source_id)
array(0,0).
% column(column_id, array_id)
column(0,0). column(1,0). column(2,0). column(3,0).
% row(row_id, array_id)
row(0,0). row(1,0). row(2,0). row(3,0).
% cell(cell_id, row_id, col_id)
cell(0,0,0). cell(1,1,0). cell(2,2,0). cell(3,3,0). cell(4,0,1). cell(5,1,1). 
cell(6,2,1). cell(7,3,1). cell(8,0,2). cell(9,1,2). cell(10,2,2). cell(11,3,2).
cell(12,0,3). cell(13,1,3). cell(14,2,3). cell(15,3,3).
% column_schema(column_schema_id,column_id,state_id,column_type,column_name,prev_column_id,prev_colum_schema_id)
column_schema(0,0,-1,"text","id",-1,-1).
column_schema(1,1,-1,"text","name",0,-1).
column_schema(2,2,-1,"text","birth_date",1,-1).
column_schema(3,3,-1,"flag","removal_flag",2,-1).
% value(value_id, cell_id, state_id, value, prev_content_id)
value(0,0,-1,"1",-1). value(1,1,-1,"1",-1). value(2,2,-1,"2",-1). value(3,3,-1,"3",-1).
value(4,4,-1,"John",-1). value(5,5,-1,"Doe",-1). value(6,6,-1,"Alex",-1). value(7,7,-1,"Patricia",-1).
value(8,8,-1,"Aug, 1 1988",-1). value(9,9,-1,"1-Aug-1986",-1).
value(10,10,-1,"20-Jan-1993",-1). value(11,11,-1,"Feb 11,1990",-1).
value(12,12,-1,"0",-1). value(13,13,-1,"0",-1). value(14,14,-1,"0",-1). value(15,14,-1,"0",-1).


% state(state_id, array_id, prev_state_id)
state(0,0,-1). 
state(1,0,-1). 
state(2,0,-1).
state(3,0,2). 
state(4,0,2).
% user(user_id, user_name)
user(0,"user_a"). 
user(1,"user_b"). 
user(2,"user_c").
user(3,"integrator_1").
user(4,"integrator_2").
% operation(operation_id, user_id, input_state_id, output_state_id)
operation(0, 0, -1, 0). 
operation(1, 1, -1, 1).
operation(2, 2, -1, 2). 
operation(3, 0, 2, 3).
operation(4, 1, 2, 4).
% changed value
%value(10,1,0,null,1). 
%value(11,5,0,null,5). 
%value(12,9,0,null,9).
%value(13,0,1,null,0). 
%value(14,4,1,null,4). 
%value(15,8,1,null,8).
% removal
value(16,13,0,"1",13).
value(17,12,1,"1",12).
value(18,1,2,"4",1). 
value(19,9,3,"1986-08-01",9). 
value(20,10,3,"1993-01-20",10).
value(21,8,4,"1988-08-01",8). 
value(22,11,4,"1990-02-11",11).


% start workflow/process model
% column_schema
column_schema(cs1,name,"id"). column_schema(cs1,data_type,text).
column_schema(cs2,name,"name"). column_schema(cs2,data_type,text).
column_schema(cs3,name,"birth_date"). column_schema(cs3,data_type,text).

% standard process
process(p1,"user a change id (keep first)").
process_input(p1,0,cs1).
process_input(p1,1,cs2).
process_input(p1,2,cs3).
process_output(p1,0,cs1).
process_output(p1,1,cs2).
process_output(p1,2,cs3).


process(p2,"user b change id (keep last)").
process_input(p2,0,cs1).
process_input(p2,1,cs2).
process_input(p2,2,cs3).
process_output(p2,0,cs1).
process_output(p2,1,cs2).
process_output(p2,2,cs3).

process(p3,"user c change id (keep both)").
process_input(p3,0,cs1).
process_output(p3,0,cs1).

process(p4,"user a change date (date type 1)").
process_input(p4,0,cs3).
process_output(p4,0,cs3).

process(p5,"user b change date (date type 2)").
process_input(p5,0,cs3).
process_output(p5,0,cs3).

process_in_out(p1,cs1,cs1).
process_in_out(p1,cs2,cs2).
process_in_out(p1,cs3,cs3).

process_in_out(p2,cs1,cs1).
process_in_out(p2,cs2,cs2).
process_in_out(p2,cs3,cs3).

process_in_out(p3,cs1,cs1).

process_in_out(p4,cs3,cs3).

process_in_out(p5,cs3,cs3).

recipe(r1,"dc workflow 1").

recipe(r1,1,p1,0).
recipe(r1,2,p2,0).
recipe(r1,3,p3,0).
recipe(r1,4,p4,3).
recipe(r1,5,p5,3).


%recipe(r2,3,p3,0).
%recipe(r2,4,p4,3).
%recipe(r2,5,p5,4).


