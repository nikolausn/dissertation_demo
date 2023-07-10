#include "recipe_model_tapp23_analysis.pl".

% implementation of a column
% column(DatasetId,ColumnId)
% there is a dataset of DatasetId that has column ColumnId
% has_schema(ColumnId,SchemaId)
% a column ColumnId has schema SchemaId
column(ds1,c1).
column(ds1,c2).
column(ds1,c3).
has_schema(c1,cs1).
has_schema(c2,cs3).
has_schema(c3,cs5).

% check hypotetical execution
% execution(HId,RecipeId,DatasetId)
% there is a hypotetical scenario HId, where a recipe Recipe Id execute on a dataset DatasetId
execution(h1,ds1,r1).


% execution violation
dataset_has_schema(DatasetId,ColumnId,SchemaId) :-
    column(DatasetId,ColumnId),
    has_schema(ColumnId,SchemaId).

execution_violation(HId,Rid,DatasetId,SchemaId,Key,Value) :-
    minimal_input_schema_recipe(Rid,SchemaId,Key,Value),
    execution(HId,DatasetId,Rid),
    not dataset_has_schema(DatasetId,_,SchemaId).

%#show execution_violation/6.