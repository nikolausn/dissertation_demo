#include "collaborative_model_facts.pl".

% recipe is also a process
process(RecipeId,Name) :-
recipe(RecipeId,Name).

% created_by(ProcessId,UserId)
% a process ProcessId was created by UserId
created_by(p1,0).
created_by(p2,1).
created_by(p3,2).
created_by(p4,0).
created_by(p5,1).
% recipe created_by
created_by(r1,3).

recipe(new_r2,"Merge workflow 1").
recipe(new_r2,3,p3,0).
recipe(new_r2,5,p5,3).
recipe(new_r2,4,p4,5).
% new recipe derived from
created_by(new_r2,4).
% derived_from(ProcessId,PrevProcessId)
% a new ProcessId was derived from PrevProcessId
derived_from(new_r2,r1).

% process attribution query
% for a workflow how many users contributes to the development
% based on the processes
attribution(Rid,Pid,ProcessName,UserId,UserName) :-
created_by(Pid,UserId),
recipe(Rid,_,Pid,_),
user(UserId,UserName),
process(Pid,ProcessName).

%#show attribution/5.

tc_process(RecipeId,RecipeId) :-
process(RecipeId,_).

tc_process(RecipeId,PrevRecipe) :-
derived_from(Y,PrevRecipe),
tc_process(RecipeId,Y).

test_tx(X,Y) :-
tc_process(X,Y),
X!=Y.

%#show tc_process/2.

% attribution for changes workflow 
attribution(Rid,Rid,ProcessName,UserId,UserName) :-
created_by(Rid,UserId),
user(UserId,UserName),
recipe(Rid,ProcessName).

attribution(Rid,PrevRecipe,ProcessName,UserId,UserName) :-
created_by(Rid,_),
tc_process(Rid,PrevRecipe),
user(UserId,UserName),
created_by(PrevRecipe,UserId),
recipe(PrevRecipe,ProcessName).

%#show workflow_attribution/5.

%attribution(Rid,UserId,UserName,Pid,ProcessName) :-
%recipe(Rid,_,Pid,_),
%user(UserId,UserName),
%process(Pid,ProcessName).


% reporting changes
user_changes(UserId,UserName,StateId,ColumnName,RowId,ValueBefore,ValueAfter) :-
value(ValueId,CellId,StateId,ValueAfter,PrevValueId),
operation(_,UserId,_,StateId),
user(UserId,UserName),
value(PrevValueId,CellId,_,ValueBefore,_),
cell(CellId,RowId,ColumnId),
column(ColumnId,_),
row(RowId,_),
column_schema(ColumnSchemaId,ColumnId,_,_,ColumnName,_,_).

%#show user_changes/7.

% merging dataset
combined_path_serial(StateStart,StateEnd,0,StateStartIn,StateEndIn,Cons,cons(StateStartIn,StateEndIn,J,Cons,User)) :-
combined_path_sink(StateStart,StateEnd,N,cons(StateStartIn,StateEndIn,J,Cons,User)).

combined_path_serial(StateStart,StateEnd,N+1,StateStartIn,StateEndIn,Cons,Signature) :-
combined_path_serial(StateStart,StateEnd,N,_,_,cons(StateStartIn,StateEndIn,J,Cons,User),Signature).

%#show combined_path_serial/7.

% hypotetical merge user_changes
% merge changes based on the dataset state
% hypotetical_merge(MergeId,StateId).
% one MergeId merge merge cells from many StateId
% for a hypotetical merge MergeId, combine changes from the collection of StateId
hypotetical_merge(m1,2).
hypotetical_merge(m1,3).
hypotetical_merge(m1,4).


% snapshot for hypotetical merge
% cell values that are not changed 

% auxiliary for cell that changes 
not_hypotetical_changes(MergeId,CellId) :-
hypotetical_merge(MergeId,StateId),
value(ValueId,CellId,StateId,ValueAfter,PrevValueId).

%#show not_hypotetical_changes/2.

merge_snapshot(MergeId,CellId,-1,Value) :-
value(ValueId,CellId,-1,Value,PrevValueId),
hypotetical_merge(MergeId,_),
not not_hypotetical_changes(MergeId,CellId).

% cell values from the hypotetical merge changes
merge_snapshot(MergeId,CellId,StateId,Value) :-
value(ValueId,CellId,StateId,Value,PrevValueId),
hypotetical_merge(MergeId,StateId).

%#show merge_snapshot/4.


% initial schema snapshot
schema_snapshot_at_state(ColumnId,StateId,ColumnSchemaId,ColumnName) :-
column_schema(ColumnSchemaId,ColumnId,StateId,_,ColumnName,_,_),
StateId=-1.

% column that is not changing schema from previous one
schema_snapshot_at_state(ColumnId,StateId,PrevColumnSchemaId,ColumnName) :-
schema_snapshot_at_state(ColumnId,StateId,PrevColumnSchemaId,ColumnName),
not column_schema(_,ColumnId,StateId,_,_,_,PrevColumnSchemaId).

% change of schema
schema_snapshot_at_state(ColumnId,StateId,ColumnSchemaId,ColumnName) :-
column_schema(ColumnSchemaId,ColumnId,StateId,_,ColumnName,_,_).

%#show schema_snapshot_at_state/4.

value_conflict(A1,A2,B1,C1,C2,D1,D2):-value(A1,B1,C1,D1,E1),
            value(A2,B2,C2,D2,E2),
            E1=E2,
            B1=B2,
            D1<D2.

branch_workflow(A1,A2,B1,B2,D1,D2):-operation(A1,B1,C1,D1),
    operation(A2,B2,C2,D2),
    C1=C2,
    A1<A2.

% any endpoint state
operation_edge(A,B) :- operation(_,_,A,B).
operation_node(X) :- operation_edge(X,_).
operation_node(X) :- operation_edge(_,X).

% start and sink of any operation_edge
operation_edge_start(X,H):- operation_node(X),H==0,H=#count{C:operation_edge(C,X)}.
operation_edge_sink(X,H) :- operation_node(X),H==0,H=#count{C:operation_edge(X,C)}.

% combined_path
combined_path(X,X,0,cons(empty)) :- operation_node(X).
combined_path(X,J,C+1,cons(X,T,J,L,N)) :- operation_edge(X,T), combined_path(T,J,C,L), operation(_,U,_,T), user(U, N).

combined_path_sink(A,B,C,D) :- combined_path(A,B,C,D),
    operation_edge_start(A,_),
    operation_edge_sink(B,_).

%#show combined_path_sink/4.


% dependency graph for each sink
path(X,X):-operation_edge(X,_).
path(X,X):-operation_edge(_,X).
path(X,Z):-operation_edge(X,Y),path(Y,Z).

% snapshot for each sink
path_sink(A,X) :- path(A,X),
    operation_edge_sink(X,_).

snapshot_value_not(E,X) :- value(A,B,C,D,E),
    path_sink(C,X).
snapshot_value(A,B,X,D,E) :- value(A,B,C,D,E),
    path_sink(C,X),
    not snapshot_value_not(A,X).
snapshot_value_diff(B5,B6,C1,C2,D1,D2) :-
    snapshot_value(A1,B1,C1,D1,E1),
    snapshot_value(A2,B2,C2,D2,E2),
    operation(A3,B3,C3,D3), operation(A4,B4,C4,D4),
    user(A5,B5), user(A6,B6), A5=B3, A6=B4,    
    D3=C1, D4=C2, C1<C2, B1=B2, D1!=D2.
value_diff_count(C1,D1,H) :- snapshot_value_diff(_,_,C1,D1,_,_),
     H=#count{E1,F1: snapshot_value_diff(_,_,C1,D1,E1,F1)}.

snapshot_value_diff_select(B5,B6,C1,C2,D1,D2) :-
snapshot_value_diff(B5,B6,C1,C2,D1,D2),
C1=3,
C2=4.

%#show snapshot_value_diff_select/6.
%#show value_diff_count/3.

snapshot_value_3_4(A,B,C,D,E,F):-
    snapshot_value_diff(A,B,C,D,E,F),
    C==3,
    D==4.
snapshot_value_3_4(A,B,C,D,E,F):-
    snapshot_value_diff(A,B,C,D,E,F),
    C==4,
    D==3.

snapshot_value_3_4(A,B,C,D,E,F):-
    snapshot_value_diff(A,B,C,D,E,F),
    C==3,
    D==4.

% #show snapshot_value_3_4/6.

% #show operation_edge/2.
% #show operation_edge_sink/2.
% #show path/2.
% #show path4/2.
% #show snapshot_value_not/2.

%#show snapshot_value/5.
%#show value_conflict/7.
%#show branch_workflow/6.