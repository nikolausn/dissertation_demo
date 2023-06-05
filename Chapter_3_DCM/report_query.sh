dbname=$1
state=3

echo "What is the name of the archive dataset ?"
#sql="select source_url,source_format from source"
sqlite3 $dbname <<EOT
.mode line
select source_url,source_format from source
EOT

echo

echo "How many data transformation steps are there in the dataset?"
sqlite3 $dbname <<EOT
.mode line
select source_url,count(1) as num_steps from(
select * from state
NATURAL JOIN source,array
)group by source_url
EOT

echo
echo "How many cells affected for each transformation step?"
sqlite3 $dbname <<EOT
.headers on
.mode columns
SELECT (state_id-(select max(state_id)+1 from state s))*-1 as state,substr(command,33) as operation,col_id,count(1) as cell_changes FROM state 
NATURAL JOIN content NATURAL JOIN value  NATURAL JOIN cell NATURAL JOIN state_command
group by state,col_id
order by state asc
EOT

echo "Show the column schema changes from the step 7 to the step 8 (split column)!"
sqlite3 $dbname <<EOT
.headers on
.mode columns
drop view col_each_state;
create  view col_each_state as
select (b.state_id-(select max(state_id) from state s))*-1 as state
,b.state_id,a.col_schema_id,a.col_id,a.col_name,a.prev_col_id,a.prev_col_schema_id 
from column_schema a, (
WITH RECURSIVE
state_cnt(state_id) AS ( 
SELECT -1 UNION ALL 
SELECT state_id+1 FROM state_cnt
  LIMIT (select max(state_id)+2 from state)
)
SELECT state_id FROM state_cnt
) b
where a.state_id<=b.state_id 
and a.col_schema_id not in
(
select a.prev_col_schema_id from column_schema a
where a.state_id<=b.state_id
)
and prev_col_id>=-1
order by state asc;

SELECT state,col_name FROM (WITH RECURSIVE
col_state_order(state,col_id,col_name,prev_col_id,level) AS (
 select state,col_id,col_name,prev_col_id,0 from col_each_state 
  where prev_col_id=-1
 UNION ALL
  SELECT a.state,a.col_id, a.col_name, a.prev_col_id,b.level+1 
   FROM col_each_state a, col_state_order b
  WHERE a.prev_col_id=b.col_id and a.state=b.state)
SELECT state,col_id,col_name,prev_col_id,level from col_state_order
where state in (7,8)
order by state asc)

EOT

echo "Which columns are being renamed?"
sqlite3 $dbname <<EOT
.headers on
.mode line
select (a.state_id-(select max(state_id)+1 from state s))*-1 as state,a.col_id,a.col_name as prev_col_name,b.col_name as new_col_name 
from column_schema a,column_schema b where a.prev_col_schema_id=b.col_schema_id
and a.col_id=b.col_id and a.col_name<>b.col_name
order by state asc;
EOT

echo "Wich rows are being removed?"
sqlite3 $dbname <<EOT
.headers on
.mode column
select (state_id-(select max(state_id)+1 from state s))*-1 as state,row_id,col_id,col_name,value_text from
(
select a.state_id,b.row_id,b.col_id,d.col_name, e.value_text from content a
NATURAL JOIN cell b
NATURAL JOIN (select state_id,row_id,count(1) as num_col from(
select a.state_id,b.row_id,b.col_id from content a,cell b
where a.cell_id = b.cell_id 
and a.state_id>=0
) group by state_id,row_id having num_col>1) c
NATURAL JOIN col_each_state d
NATURAL JOIN value e
)
order by state,row_id asc;
EOT

echo "Show dependency of step 4 or step 8!"
sqlite3 $dbname <<EOT
.headers on
.mode column
drop view col_dep_state;
create view col_dep_state as
WITH RECURSIVE
col_dep_order(state_id,prev_state_id,prev_input_column,input_column,output_column,level) AS (
 select state_id,state_id,input_column,input_column,output_column,0 from col_dependency cd 
 UNION ALL
  SELECT a.state_id,b.state_id,b.prev_input_column,a.input_column,b.input_column,b.level+1
   FROM col_dependency a, col_dep_order b
  WHERE a.output_column=b.input_column 
  and a.state_id>b.state_id)
SELECT distinct * from col_dep_order 
order by prev_input_column;

select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,(b.state_id-(select max(state_id)+1 from state s))*-1 as dep_state,substr(d.command,33) as dep_command 
from col_dependency a,col_dep_state b,state_command c,state_command d
where a.input_column = b.prev_input_column
and a.state_id>-1
and a.state_id<b.state_id
and a.state_id=c.state_id 
and b.state_id=d.state_id
and state in (4,8)
order by state,dep_state asc;
EOT