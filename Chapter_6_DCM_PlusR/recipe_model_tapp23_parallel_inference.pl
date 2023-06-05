#include "recipe_model_tapp23_analysis.pl".

recipe_input(Rid,N,Pid,CSin) :-
    recipe(Rid,N,Pid,_),
    process_input(Pid,_,CSin).

recipe_output(Rid,N,Pid,CSout) :-
    recipe(Rid,N,Pid,_),
    process_output(Pid,_,CSout).

recipe_conflict(Rid,N,Pid1,M,Pid2) :-
    recipe_input(Rid,M,Pid2,CSin),
    recipe_input(Rid,N,Pid1,CSin),
    M>N.

recipe_conflict(Rid,N,Pid1,M,Pid2) :-
    recipe_input(Rid,M,Pid2,CSin),
    recipe_output(Rid,N,Pid1,CSin),
    M>N.

%#show recipe_conflict/5.

parallel_candidate(Rid,N,Pid) :-
    recipe(Rid,N,Pid,_),
    not recipe_conflict(Rid,_,_,N,_).

parallel_process(Rid,seq(0,none),seq(N,Pid),seq(0,N,none)) :-
    parallel_candidate(Rid,N,Pid).

parallel_process(Rid,seq(N,Pid1),seq(M,Pid2),seq(N,M,Sign)) :-
    parallel_process(Rid,_,seq(N,Pid1),Sign),
    recipe_output(Rid,N,Pid1,CSout),
    recipe_input(Rid,M,Pid2,CSout),
    M>N.

%parallel_process_count(Rid,A,B,H):-
%    parallel_process(Rid,A,B,_),
%    H=#count{X:parallel_process(Rid,A,B,X)}.

temp_serialize_process(Rid,seq(N,Pid1),seq(M,Pid2),N,M,Sign) :-
    parallel_process(Rid,seq(N,Pid1),seq(M,Pid2),seq(N,M,Sign)).

temp_serialize_process(Rid,seq(N,Pid1),seq(M,Pid2),J,K,Sign) :-
    temp_serialize_process(Rid,seq(N,Pid1),seq(M,Pid2),_,_,seq(J,K,Sign)).

%#show temp_serialize_process/6.

parallel_process_not(Rid,J,M)  :-
    temp_serialize_process(Rid,seq(N,Pid1),seq(M,Pid2),J,K,Sign),
    J!=N.

%#show parallel_process_not/3.


parallel_recipe(Rid,N,M,Pid1,Pid2):-
    parallel_process(Rid,seq(N,Pid1),seq(M,Pid2),_),
    not parallel_process_not(Rid,N,M).

%    parallel_process_count(Rid,seq(N,Pid1),seq(M,Pid2),1).

%#show recipe_input/4.
%#show parallel_candidate/3.
%#show parallel_process/4.
#show parallel_recipe/5.
%#show parallel_process_count/4.