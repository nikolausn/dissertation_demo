#include "collaborative_model_facts.pl".

% all_recipe_input(Rid,CSin)
% Recipe `Rid` "overall" has inputs `CSin` (one to many)
all_recipe_input(Rid,CSin) :-
    recipe(Rid,_,Pid,_),
    process_input(Pid,_,CSin).

% all_recipe_output(Rid,CSout)
% Recipe `Rid` "overall" has outputs `CSin` (one to many)

all_recipe_output(Rid,CSout) :-
    recipe(Rid,_,Pid,_),
    process_output(Pid,_,CSout).

%#show all_recipe_input/2.
%#show all_recipe_output/2.


%recipe_start(Rid,S1,P1) :- 
%    recipe(Rid,_,P1,S1),
%    H==0,
%    H=#count{S1:recipe(Rid,S1,_,_)}.

not_recipe_start(Rid,Sid) :-
    recipe(Rid,Sid,_,_).    
recipe_start(Rid,Sid) :- 
    recipe(Rid,_,_,Sid),
    not not_recipe_start(Rid,Sid).

% end of the recipe sequence
%recipe_sink(Rid,Sid,P1) :- 
%    recipe(Rid,Sid,P1,_),
%    H==0,
%    H=#count{Sid:recipe(Rid,_,_,Sid)}.

not_recipe_sink(Rid,Sid) :-
    recipe(Rid,_,_,Sid).
recipe_sink(Rid,Sid) :- 
    recipe(Rid,Sid,_,_),
    not not_recipe_sink(Rid,Sid).

%#show recipe_start/3.
%#show recipe_sink/3.

recipe_edge(Rid,recipe(RStart,null),recipe(RSeq1,Pid1),seq(RStart,RSeq1),0,RSeq1) :- 
    recipe(Rid,RSeq1,Pid1,RStart),
    recipe_start(Rid,RStart).
recipe_edge(Rid,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2),RSeq1,RSeq2) :- 
    recipe(Rid,RSeq1,Pid1,_),
    recipe(Rid,RSeq2,Pid2,RSeq1),
    RSeq1!=RSeq2.

%#show recipe_edge/6.

recipe_path(Rid,Pid1,Pid2,seq(RSeq1,RSeq2,0),RSeq1,RSeq2) :- 
    recipe_edge(Rid,Pid1,Pid2,seq(RSeq1,RSeq2),RSeq1,RSeq2),
    RSeq1!=RSeq2.

recipe_path(Rid,Pid1,Pid3,seq(seq(RSeq3,RSeq4),seq(RSeq1,RSeq2,N),N+1),RR3,RR2) :- 
    recipe_edge(Rid,Pid1,Pid2,seq(RSeq3,RSeq4),RR3,RR4),
    recipe_path(Rid,Pid2,Pid3,seq(RSeq1,RSeq2,N),RR4,RR2),
    RR3!=RR4,
    RR4!=RR2,
    RR3!=RR2.

%#show recipe_path/6.


recipe_path_select(h1,Pid1,Pid3,Sign,RR3,RR2) :- 
    recipe_path(h1,Pid1,Pid3,Sign,RR3,RR2).

recipe_edge_select(h1,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2),RSeq1,RSeq2):-
    recipe_edge(h1,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2),RSeq1,RSeq2). 


%#show recipe_edge_select/6.
%#show recipe_path_select/6.


select_recipe_path(Rid,recipe(S1,Pid1),recipe(S2,Pid2),Sign,RSeq1,RSeq2) :- 
    recipe_path(Rid,recipe(S1,Pid1),recipe(S2,Pid2),Sign,RSeq1,RSeq2),
    recipe_start(Rid,S1),
    recipe_sink(Rid,S2).

%#show select_recipe_path/6.

serialize_recipe_path(Rid,recipe(SS1,null),SS2,S1,N,S2,seq(S1,S2,N),0) :-
    select_recipe_path(Rid,recipe(SS1,null),SS2,seq(S1,S2,N),_,_).

serialize_recipe_path(Rid,Pid1,Pid2,S1,N,S2,Sign,M+1) :-
    serialize_recipe_path(Rid,Pid1,Pid2,_,_,seq(S1,S2,N),Sign,M).

%#show serialize_recipe_path/8.

select_serialize_recipe(Rid,A,B,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,S1,M,S2,Sign,N),
    recipe(Rid,S2,Pid,_).

select_serialize_recipe(Rid,A,B,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,S1,M,seq(S2,_,_),Sign,N),
    recipe(Rid,S2,Pid,_).

select_serialize_recipe(Rid,A,B,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,S1,M,seq(seq(S2,_),_,_),Sign,N),
    recipe(Rid,S2,Pid,_).

select_serialize_recipe(Rid,A,B,S1,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,S1,M,S2,Sign,N),
    recipe(Rid,S2,Pid,_).

select_serialize_recipe(Rid,A,B,S1,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,seq(S1,_),M,seq(S2,_,_),Sign,N),
    recipe(Rid,S2,Pid,_).

select_serialize_recipe(Rid,A,B,S1,S2,Pid,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,seq(seq(S1,_),_,_),M,seq(seq(S2,_),_,_),Sign,N),
    recipe(Rid,S2,Pid,_).

%#show serialize_recipe_path/9.

test_serialize_recipe(Rid,recipe(S1,A),recipe(S2,B),S1,S2,SS,Pid,M,Sign,N) :-
    recipe_start(Rid,S1),
    recipe_sink(Rid,S2),
    select_serialize_recipe(Rid,recipe(S1,A),recipe(S2,B),SS,Pid,M,Sign,N).

%#show test_serialize_recipe/10.


% serialize recipe with previous state
test_serialize_recipe(Rid,recipe(S1,A),recipe(S2,B),S1,S2,SS1,SS2,Pid,M,Sign,N) :-
    recipe_start(Rid,S1),
    recipe_sink(Rid,S2),
    select_serialize_recipe(Rid,recipe(S1,A),recipe(S2,B),SS1,SS2,Pid,M,Sign,N).

%#show test_serialize_recipe/11.


removed_schema(Rid,SS1,Pid,CSin) :-
    recipe(Rid,SS1,Pid,_),
    process_input(Pid,_,CSin),
    not process_output(Pid,_,CSin).

derived_output(Rid,S1,S2,-1,M-1,Sign,N+1,CSin,state(-1,CSin)) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    M=0,
    recipe(Rid,SS1,Pid,_),
    process_input(Pid,_,CSin).

derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,state(SS1,CSout)) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    recipe(Rid,SS1,Pid,_),
    process_output(Pid,_,CSout).

derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,State) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    derived_output(Rid,S1,S2,_,M-1,Sign,_,CSout,State),
    not removed_schema(Rid,SS1,_,CSout),
    not process_output(Pid,_,CSout).

%#show derived_output/9.

% derived process
% derived process for last rank
derived_process(Rid,S1,S2,SS1,M,Sign,N,CSin,StateIn,Pid,CSout,StateOut) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    %N=0,
    process_input(Pid,_,CSin),
    process_output(Pid,_,CSout),
    derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,StateOut),
    % prioritize one that coming from the same signatures    
    derived_output(Rid,_,_,_,_,Sign,N+1,CSin,StateIn).

% missing input, set to initial

derived_process(Rid,S1,S2,SS1,M,Sign,N,CSin,state(-1,CSin),Pid,CSout,StateOut) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    process_input(Pid,_,CSin),
    process_output(Pid,_,CSout),
    derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,StateOut),
    not derived_output(Rid,_,_,_,_,Sign,N+1,CSin,_).

% removed column
derived_process(Rid,S1,S2,SS1,M,Sign,N,CSin,StateIn,Pid,csRemoved,state(SS1,csRemoved)) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    %N=0,
    process_input(Pid,_,CSin),
    removed_schema(Rid,SS1,Pid,CSin),
    %derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,StateOut),
    %prioritize one that coming from the same signatures    
    derived_output(Rid,_,_,_,_,Sign,N+1,CSin,StateIn).


% additional process that is not coming from the same signatures
derived_process(Rid,S1,S2,SS1,M,Sign,N,CSin,StateIn,Pid,CSout,StateOut) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    process_input(Pid,_,CSin),
    process_output(Pid,_,CSout),
    derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,StateOut),
    derived_output(Rid,_,_,_,_,_,N+1,CSin,StateIn),
    not derived_output(Rid,_,_,_,_,Sign,N+1,CSin,_).

derived_process(Rid,S1,S2,SS1,M,Sign,N,CSin,StateIn,Pid,CSout,StateOut) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS1,Pid,M,Sign,N),
    process_input(Pid,_,CSin),
    process_output(Pid,_,CSout),
    derived_output(Rid,S1,S2,SS1,M,Sign,N,CSout,StateOut),
    % cross signatures
    derived_output(Rid,_,_,_,_,SignB,_,CSin,StateIn),
    SignB!=Sign,
    not derived_output(Rid,_,_,_,_,_,N+1,CSin,_).

%%#show derived_process/12.

% read violation

intersection_process(Rid,SignA,SignB,SS1A,StateIn) :-
    derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,CSin,StateIn,PidA,CSoutA,StateOutA),
    derived_process(Rid,S1B,S2B,SS1A,MB,SignB,NB,CSin,StateIn,PidB,CSoutB,StateOutB),
    SignA!=SignB.

intersection_process(Rid,SignA,SignB,SS1A,StateOut) :-
    derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,CSinA,StateInA,PidA,CSout,StateOut),
    derived_process(Rid,S1B,S2B,SS1A,MB,SignB,NB,CSinB,StateInB,PidB,CSout,StateOut),
    SignA!=SignB.
    
%#show write_intersection_process/5.

% read violation
read_violation(Rid,S1A,S2A,S1B,S2B,SS1A,SS1B,SignA,SignB,CSin,StateIn,PidA,PidB):-
    derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,CSin,StateIn,PidA,CSoutA,StateOutA),
    derived_process(Rid,S1B,S2B,SS1B,MB,SignB,NB,CSin,StateIn,PidB,CSoutB,StateOutB),
    SignA!=SignB,    
    not intersection_process(Rid,SignA,SignB,_,StateIn).
%#show read_violation/13.

read_violation_0_1(Rid,S1A,S2A,S1B,S2B,SS1A,SS1B,SignA,SignB,CSin,StateIn,PidA,PidB):-
    read_violation(Rid,S1A,S2A,S1B,S2B,SS1A,SS1B,SignA,SignB,CSin,StateIn,PidA,PidB),
    S1A=0,
    S2A=1.
    
% write violation
write_violation(Rid,S1A,S2A,S1B,S2B,SS1A,SS1B,SignA,SignB,CSout,StateOutA,StateOutB,PidA,PidB):-
    derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,CSinA,StateInA,PidA,CSout,StateOutA),
    derived_process(Rid,S1B,S2B,SS1B,MB,SignB,NB,CSinB,StateInB,PidB,CSout,StateOutB),
    SignA!=SignB,    
    StateOutA!=StateOutB,
    CSout!=csRemoved,
    %MA=MB,
    not intersection_process(Rid,SignA,SignB,_,_).
%#show write_violation/14.

% missing dependency violation
missing_dependency(Rid,S1A,S2A,SS1A,SignA,CSin):- 
    derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,_,_,PidA,_,_),
    process_input(PidA,_,CSin),
    not derived_process(Rid,S1A,S2A,SS1A,MA,SignA,NA,CSin,_,_,_,_). 

%#show read_violation/13.


%#show missing_dependency/6.

signature_paths(Rid,Sign) :-
    test_serialize_recipe(Rid,A,B,S1,S2,SS,Pid,M,Sign,N).

no_conflict(Rid,SignA,SignB) :- 
    signature_paths(Rid,SignA), 
    signature_paths(Rid,SignB),
    not read_violation(Rid,_,_,_,_,_,_,SignA,SignB,_,_,_,_).

%#show no_conflict/3.

% temporary negation for cluster conflict
cluster_conflict_not(Rid,SignX) :-
    no_conflict(Rid,SignA,SignB),
    read_violation(Rid,_,_,_,_,_,_,SignX,SignB,_,_,_,_),
    read_violation(Rid,_,_,_,_,_,_,SignX,SignA,_,_,_,_).

%cluster_conflict_not(Rid,SignA) :-
%    no_conflict(Rid,SignA,SignB),
%    not cluster_conflict(Rid,SignA). 

%#show cluster_conflict/2.

cluster_no_conflict(Rid,SignA,SignB) :-
    no_conflict(Rid,SignA,SignB),
    cluster_conflict_not(Rid,SignA),
    H>0,
    H=#count{X:cluster_conflict_not(Rid,X)}.

cluster_no_conflict(Rid,SignA,SignB) :-
    no_conflict(Rid,SignA,SignB),
    H==0,
    H=#count{X:cluster_conflict_not(Rid,X)}.

%#show cluster_no_conflict/3.

% merging example

merge_sign(r1,h1,seq(seq(0,3),seq(3,4,0),1)).
merge_sign(r1,h1,seq(seq(0,3),seq(3,5,0),1)).

%#show merge_sign/3.

%after(Rid,Hid,S1,S2) :-
%    merge_sign(Rid,Hid,SignA),
%    test_serialize_recipe(Rid,_,_,_,_,S1,S2,_,_,SignA,_).

%#show merge_sign/3.

after(Rid,Hid,S1,S2) :-
    merge_sign(Rid,Hid,SignA),
    test_serialize_recipe(Rid,_,_,_,_,S1,S2,_,_,SignA,_).

%#show after/4.

