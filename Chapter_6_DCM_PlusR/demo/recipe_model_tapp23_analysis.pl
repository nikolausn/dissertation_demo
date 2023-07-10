#include "recipe_model_tapp23_facts.pl".


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

% recipe_edge(Rid,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2)) 
% recipe `Rid` has edge defined by source node at `RSeq1`  to `RSeq2` with detailed processes of `Pid1` and `Pid2`

recipe_edge(Rid,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2)) :- 
    recipe(Rid,RSeq1,Pid1,Rprev),
    recipe(Rid,RSeq2,Pid2,RSeq1).

recipe_edge(Rid,recipe(-1,-1),recipe(RSeq1,Pid1),seq(0,RSeq1)) :- 
    recipe(Rid,RSeq1,Pid1,0).

% additional one hop
%recipe_edge(Rid,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2)) :- 
%    recipe(Rid,RSeq1,Pid1,Rprev),
%    not recipe(Rid,_,_,RSeq1).

% recipe_path(Rid,Seq1,Seq2,seq(RSeq1,RSeq2,0))
% recipe `Rid` has path from Seq1 to Seq2 through sequences of seq(RSeq1,RSeq2,0) 
recipe_path(Rid,Seq1,Seq2,seq(RSeq1,RSeq2,0)) :- 
    recipe_edge(Rid,Seq1,Seq2,seq(RSeq1,RSeq2)).
recipe_path(Rid,Pid1,Pid3,seq(seq(RSeq3,RSeq4),seq(RSeq1,RSeq2,N),N+1)) :- 
    recipe_edge(Rid,Pid1,Pid2,seq(RSeq3,RSeq4)),
    recipe_path(Rid,Pid2,Pid3,seq(RSeq1,RSeq2,N)).

recipe_edge(Rid,recipe(-1,-1),recipe(RSeq1,Pid1),seq(0,RSeq1),0,RSeq1) :- 
    recipe(Rid,RSeq1,Pid1,0).
recipe_edge(Rid,recipe(RSeq1,Pid1),recipe(RSeq2,Pid2),seq(RSeq1,RSeq2),RSeq1,RSeq2) :- 
    recipe(Rid,RSeq1,Pid1,Rprev),
    recipe(Rid,RSeq2,Pid2,RSeq1).

recipe_path(Rid,Pid1,Pid2,seq(RSeq1,RSeq2,0),RSeq1,RSeq2) :- 
    recipe_edge(Rid,Pid1,Pid2,seq(RSeq1,RSeq2),RSeq1,RSeq2).
recipe_path(Rid,Pid1,Pid3,seq(seq(RSeq3,RSeq4),seq(RSeq1,RSeq2,N),N+1),RR3,RR2) :- 
            recipe_edge(Rid,Pid1,Pid2,seq(RSeq3,RSeq4),RR3,RR4),
            recipe_path(Rid,Pid2,Pid3,seq(RSeq1,RSeq2,N),RR4,RR2).

%#show recipe_path/6.

% serialize_recipe_path(Rid,S1,S2,P1,N,P2,seq(S1,S2,N),M)
% Recipe `Rid` from seq `S1` to `S2` executes P2 at order/sequence of `M`
serialize_recipe_path(Rid,Pid1,Pid2,S1,N,S2,seq(S1,S2,N),0) :-
    recipe_path(Rid,Pid1,Pid2,seq(S1,S2,N)).

%serialize_recipe_path(Rid,recipe(-1,-1),Pid2,S1,N,S2,seq(S1,S2,N),0) :-
%    recipe_path(Rid,recipe(-1,-1),Pid2,seq(S1,S2,N),_,_).

serialize_recipe_path(Rid,Pid1,Pid2,S1,N,S2,Sign,M+1) :-
    serialize_recipe_path(Rid,Pid1,Pid2,_,_,seq(S1,S2,N),Sign,M), N>0.

serialize_recipe_path(Rid,Pid1,Pid2,seq(S1,S2),0,-1,Sign,M+1) :-
    serialize_recipe_path(Rid,Pid1,Pid2,_,_,seq(S1,S2,N),Sign,M), N=0.

select_serialize_recipe(Rid,A,B,S1,N,Sign,M) :- 
    serialize_recipe_path(Rid,A,B,S1,M,S2,Sign,N).


% required input for a process state
select_serialize_derived_input(Rid,A,B,seq(-1,S1),M-1,Sign,N,CSin) :-
    select_serialize_recipe(Rid,A,B,seq(S1,S2),M,Sign,N),
    recipe(Rid,S1,P1,_),
    M=0,
    all_recipe_input(Rid,CSin),
    not process_input(P1,_,CSin).

select_serialize_derived_input(Rid,A,B,seq(-1,S1),M-1,Sign,N,CSout) :-
    select_serialize_recipe(Rid,A,B,seq(S1,S2),M,Sign,N),
    recipe(Rid,S1,P1,_),
    M=0,
    process_output(P1,_,CSout). 

select_serialize_derived_input(Rid,A1,B1,seq(S3,S4),M1,Sign,N1,CSin) :-
    select_serialize_recipe(Rid,A1,B1,seq(S3,S4),M1,Sign,N1),
    select_serialize_derived_input(Rid,A1,B1,seq(S1,S2),M2,Sign,N2,CSin),
    M2=M1-1,
    recipe(Rid,S4,P4,_),
    S2=S3,
    not process_input(P4,_,CSin).

select_serialize_derived_input(Rid,A1,B1,seq(S3,S4),M1,Sign,N1,CSout) :-
    select_serialize_recipe(Rid,A1,B1,seq(S3,S4),M1,Sign,N1),
    recipe(Rid,S4,P4,_),
    %M1>0,
    process_output(P4,_,CSout).

%#show serialize_recipe_path/8.

select_serialize_derived_input_temp(Rid,A1,B1,seq(S3,S4),CSout) :-
    select_serialize_derived_input(Rid,A1,B1,seq(S3,S4),M1,Sign,N1,CSout),
    S3=1,
    S4=2,
    M1=-1.

% start of recipe sequence
%recipe_start(Rid,S2,P1) :- 
%    recipe(Rid,S2,P1,S1),
%    S1=0.

recipe_start(Rid,S1,P1) :- 
    recipe(Rid,_,P1,S1),
    H==0,
    H=#count{S1:recipe(Rid,S1,_,_)}.

% end of the recipe sequence
recipe_sink(Rid,Sid,P1) :- 
    recipe(Rid,Sid,P1,_),
    H==0,
    H=#count{Sid:recipe(Rid,_,_,Sid)}.

%#show recipe_start/3.
%#show recipe_sink/3.

select_recipe_input_violation(Rid,recipe(SS1,SP1),recipe(SS2,SP2),seq(S1,S2),M,Sign,N,P1,CSin) :-
    select_serialize_recipe(Rid,recipe(SS1,SP1),recipe(SS2,SP2),seq(S1,S2),M,Sign,N),
    recipe(Rid,S2,P1,_),
    %M>0,
    process_input(P1,_,CSin),
    recipe_start(Rid,SS1,_),
    recipe_sink(Rid,SS2,_),
    not select_serialize_derived_input(Rid,recipe(SS1,SP1),recipe(SS2,SP2),seq(_,S1),M-1,Sign,_,CSin).


#show select_recipe_input_violation/9.


test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,seq(SS1,SS2,SS3),N) :-
    test_serialize_recipe(Rid,A,B,S1,S2,S1,M,seq(SS1,SS2,SS3),N).

test_serialize_recipe(Rid,A,B,S1,S2,SS,M,Sign,N) :-
    recipe_start(Rid,S1,_),
    recipe_sink(Rid,S2,_),
    recipe_path(Rid,_,_,Sign,S1,S2),
    select_serialize_recipe(Rid,A,B,SS,M,Sign,N).

test_select_serialize_derived_input(Rid,A1,B1,S1,S2,seq(S3,S4),M1,Sign,N1,CSin) :-
    select_serialize_derived_input(Rid,A1,B1,seq(S3,S4),M1,Sign,N1,CSin),
    test_serialize_recipe(Rid,A1,B1,S1,S2,SS,M,Sign,N1).

test_serialize_recipe_dup(Rid,seq(SS1,SS2),Sign1):-
    test_serialize_recipe(Rid,_,_,_,_,seq(SS1,SS2),_,Sign1,_),
    test_serialize_recipe(Rid,_,_,_,_,seq(SS1,SS2),_,Sign2,_),
    Sign1 != Sign2.

test_serialize_recipe_unique(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N):-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    not test_serialize_recipe_dup(Rid,seq(SS1,SS2),Sign).

%#show test_serialize_recipe_unique/9.
% look for any duplicate or subset of process
%#show test_serialize_recipe_dup/3.

%parallel_recipe_violation(Rid,A1,B1,A2,B2,S1,S2,S3,S4,Sign1,Sign2,seq(SS2,PP1),seq(SS4,PP2),CS1) :-
%    test_serialize_recipe_unique(Rid,A1,B1,S1,S2,seq(SS1,SS2),M1,Sign1,_),
%    test_serialize_recipe_unique(Rid,A2,B2,S3,S4,seq(SS3,SS4),M2,Sign2,_),
%    Sign1!=Sign2,
%    recipe(Rid,SS2,PP1,_),
%    recipe(Rid,SS4,PP2,_),
%    process_input(PP1,_,CS1),
%    process_output(PP2,_,CS1),
%    SS2>SS4.

%#show parallel_recipe_violation/14.


minimal_input(Rid,S1,S2,seq(-1,SS1),M-1,Sign,N,CSin) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    M=0,
    recipe(Rid,SS1,Pid,_),
    process_input(Pid,_,CSin).

derived_output(Rid,S1,S2,seq(-1,SS1),M-1,Sign,N,CSout) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    M=0,
    recipe(Rid,SS1,Pid,_),
    process_output(Pid,_,CSout),
    not removed_schema(Rid,SS1,_,CSout).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
%    %M>0,
    recipe(Rid,SS2,Pid,_),
    process_output(Pid,_,CSout),
    not removed_schema(Rid,SS2,_,CSout).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    derived_output(Rid,S1,S2,seq(_,SS1),M-1,Sign,_,CSout),
    not removed_schema(Rid,SS2,_,CSout).

% expanded with derived sign

derived_output(Rid,S1,S2,seq(-1,SS1),M-1,Sign,N+1,CSout,schema_state(CSin,-1),schema_state(CSout,SS1),Pid,SS1) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    M=0,
    recipe(Rid,SS1,Pid,_),
    %process_input(Pid,_,CSin),
    %process_output(Pid,_,CSout),
    process_in_out(Pid,CSin,CSout),
    not removed_schema(Rid,SS1,_,CSout).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,schema_state(CSin,-1),schema_state(CSout,SS2),Pid,SS2) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
%    %M>0,
    recipe(Rid,SS2,Pid,_),
    %process_input(Pid,_,CSin),
    %process_output(Pid,_,CSout),
    process_in_out(Pid,CSin,CSout),
    not derived_output(Rid,_,_,_,M-1,Sign,_,CSin),
    not removed_schema(Rid,SS2,_,CSout).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,schema_state(CSin,MM),schema_state(CSout,SS2),Pid,SS2) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    recipe(Rid,SS2,Pid,_),
    derived_output(Rid,S1,S2,_,M-1,Sign,_,CSin,Min,schema_state(CSin,MM),_,_),
    %process_input(Pid,_,CSin),
    %process_output(Pid,_,CSout),
    process_in_out(Pid,CSin,CSout),
    CSin!=csRemoved,
    not removed_schema(Rid,SS2,_,CSout).

%derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,csRemoved,schema_state(CSin,MM),schema_state(csRemoved,SS2),Pid,SS2) :-
%    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
%    recipe(Rid,SS2,Pid,_),
%    derived_output(Rid,S1,S2,_,M-1,Sign,_,CSin,Min,schema_state(CSin,MM),_,_),
    %process_input(Pid,_,CSin),
%    process_in_out(Pid,_,CSin),
%    H==0,
%    H=#count{CSout: process_output(Pid,_,CSout)}.
    %removed_schema(Rid,SS2,_,CSin).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,csRemoved,schema_state(CSin,MM),schema_state(csRemoved,SS2),Pid,SS2) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    recipe(Rid,SS2,Pid,_),
    derived_output(Rid,S1,S2,_,M-1,Sign,_,CSin,Min,schema_state(CSin,MM),_,_),
    process_in_out(Pid,CSin,csRemoved).

% carry over from previous level
derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidOld,SS2Old) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    derived_output(Rid,S1,S2,seq(_,SS1),M-1,Sign,_,CSout,Min,Mout,PidOld,SS2Old),
    recipe(Rid,SS2,Pid,_),
    not process_output(Pid,_,CSout),
    not removed_schema(Rid,SS2,_,CSout).

derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidOld,SS2Old) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    derived_output(Rid,S1,S2,seq(_,SS1),M-1,SignB,_,CSout,Min,Mout,PidOld,SS2Old),
    recipe(Rid,SS2,Pid,_),
    Sign!=SignB,
    not removed_schema(Rid,SS2,_,CSout).

 

%derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
%    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
%    not derived_output(Rid,_,_,_,_,_,N+1,_,Min,Mout,_,_).

derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
    derived_output(Rid,_,_,_,_,_,N+1,_,MinPrev,Min,_,_),
    not derived_output(Rid,_,_,_,_,_,N+1,_,Min,Mout,_,_).

%derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,schema_state(CSin,-1),Mout,PidM,SS2M) :-
%    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,schema_state(CSin,-1),Mout,PidM,SS2M).

%derived_output_inv_not(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
%    derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
%    derived_output_inv(Rid,_,_,_,_,_,N+1,_,_,Min,_,_).

%#show derived_output_inv_not/12.

%derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M) :-
%    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M),
%    derived_output_inv_not(Rid,_,_,_,_,_,N,_,Mout,MoutNext,_,_).

derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M) :-
    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M),
    derived_output(Rid,_,_,_,_,_,N,_,Mout,MoutNext,_,_).
    %not derived_output_inv(Rid,_,_,_,_,_,N,Min,Mout,_,_,_).

derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M) :-
    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N+1,CSout,Min,Mout,PidM,SS2M),
    derived_output(Rid,_,_,_,_,_,N,_,Mout,MoutNext,_,_).

%derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
%    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
%    not derived_output_inv(Rid,_,_,_,_,_,N,_,Mout,MoutNext,_,_).

derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
    derived_process_addin(Rid,N,_,_,Min,Mout,_,_,_,_).

%derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
%    derived_output_inv_addin(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M).

% derived processes attempt to handle input output process ranking using process state
derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA) :-
    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,MoutNext,PidMA,SS2MA),
    derived_output(Rid,S1B,S2B,seq(SS1B,SS2B),MB,SignB,NB,CSoutB,Min,Mout,PidMB,SS2MB),
    NB>NA,
    SS2MB!=SS2MA.

% derived processes attempt to handle input output process ranking using process state
%derived_process(Rid,NA,-1,schema_state(null,null),Mout,schema_state(csRemoved,SSNext),-1,-1,PidMA,SS2MA) :-
%    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,schema_state(csRemoved,SSNext),PidMA,SS2MA).

%derived_process(Rid,NA,NB,Min,Mout,MoutNextC,PidMB,SS2MB,PidMA,SS2MA) :-
%    derived_output(Rid,_,_,_,_,_,NA,_,Mout,MoutNextC,PidMA,SS2MA),
%    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,MoutNext,PidMA,SS2MA),
%    derived_output(Rid,S1B,S2B,seq(SS1B,SS2B),MB,SignB,NB,CSoutB,Min,Mout,PidMB,SS2MB),
%    NB>NA,
%    SS2MB!=SS2MA.


%derived_process(Rid,0,NA,Mout,MoutNext,schema_state(-1,-1),PidMA,SS2MA,-1,-1) :-
%    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,MoutNext,PidMA,SS2MA),
%    not derived_output(Rid,_,_,_,_,_,_,_,_,Mout,_,_).

derived_process_addin(Rid,NA,-1,-1,Mout,MoutNext,-1,-1,PidMA,SS2MA) :-
    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,MoutNext,PidMA,SS2MA),
    not derived_process(Rid,_,_,_,_,_,_,_,_,SS2MA),
    not derived_process(Rid,_,_,_,_,_,_,SS2MA,_,_).

derived_process_addin(Rid,NA,NB,-1,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA) :-
    derived_output(Rid,S1A,S2A,seq(SS1A,SS2A),MA,SignA,NA,CSoutA,Mout,MoutNext,PidMA,SS2MA),
    derived_process_clean(Rid,NA,NB,_,_,_,PidMB,SS2MB,PidMA,SS2MA),
    not derived_process(Rid,_,_,_,_,MoutNext,_,_,_,_).

derived_process_addin_max(Rid,HNA,HNB,SS2MB,SS2MA) :-
    derived_process_addin(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    HNB=#max{MaxNB: derived_process_addin(Rid,_,MaxNB,_,_,_,_,SS2MB,_,SS2MA)},
    HNA=#max{MaxNA: derived_process_addin(Rid,MaxNA,HNB,_,_,_,_,SS2MB,_,SS2MA)}.

derived_process_clean(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA) :-
    derived_process_addin(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    derived_process_addin_max(Rid,NA,NB,SS2MB,SS2MA).
    %not parallel_recipe_violation(Rid,_,_,_,_,_,_,PidMB,_,_,_,SS2MB).
    
derived_process_not(Rid,NA,NB,NB2,SS2MB2,SS2MB) :-
    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    derived_process(Rid,NA,NB2,_,_,_,_,SS2MB2,_,SS2MB).

derived_process_clean(Rid,NB,-1,-1,Min,Mout,-1,-1,PidMB,SS2MB) :-
    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    derived_process_max(Rid,NA,NB,SS2MB,SS2MA),
    not derived_process(Rid,_,_,_,_,_,_,_,_,SS2MB),
    not derived_process_not(Rid,NA,_,NB,SS2MB,SS2MA).

%derived_process_max(Rid,NA,H,SS2MB,SS2MA) :-
%    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
%    H=#max{MaxNB: derived_process(Rid,NA,MaxNB,_,_,_,_,SS2MB,_,SS2MA)}.

derived_process_max(Rid,HNA,HNB,SS2MB,SS2MA) :-
    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    HNB=#max{MaxNB: derived_process(Rid,_,MaxNB,_,_,_,_,SS2MB,_,SS2MA)},
    HNA=#max{MaxNA: derived_process(Rid,MaxNA,HNB,_,_,_,_,SS2MB,_,SS2MA)}.

derived_process_max(Rid,H,SS2MB,SS2MA) :-
    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    H=#max{MaxNB: derived_process(Rid,_,MaxNB,_,_,_,_,SS2MB,_,SS2MA)}.

derived_process_clean(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA) :-
    derived_process(Rid,NA,NB,Min,Mout,MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    derived_process_max(Rid,NA,NB,SS2MB,SS2MA),
    %derived_process_max(Rid,NB,SS2MB,SS2MA),
    not derived_process_not(Rid,NA,_,NB,SS2MB,SS2MA).
    %not parallel_recipe_violation(Rid,_,_,_,_,_,_,_,_,_,_,SS2MB).

derived_process_schema(Rid,NMax+1,schema_state(CSin,-1),PidMB,SS2MB,CSin) :-
    derived_process_clean(Rid,NA,NB,Min,schema_state(CSin,-1),MoutNext,PidMB,SS2MB,PidMA,SS2MA),
    max_derived_output_n(Rid,NMax).

derived_process_schema_removed(Rid,N,schema_state(CSin,SSin),CSin) :-
    derived_process_clean(Rid,N,NB,Min,schema_state(CSin,SSin),schema_state(csRemoved,SSout),PidMB,SS2MB,PidMA,SS2MA).

derived_process_schema(Rid,N,schema_state(CSin,SSout),PidMA,SS2MA,CSin) :- 
    derived_process_clean(Rid,N,_,_,schema_state(CSin,SSIn),schema_state(CSin,SSout),_,_,PidMA,SS2MA).

derived_process_schema(Rid,N,schema_state(CSout,SSout),PidMA,SS2MA,CSout) :- 
    derived_process_clean(Rid,N,_,_,schema_state(CSin,SSIn),schema_state(CSout,SSout),_,_,PidMA,SS2MA),
    not parallel_recipe_violation(Rid,_,_,_,_,_,_,PidMA,_,_,_,SS2MA),
    CSin!=CSout,
    CSout!=csRemoved,
    not derived_process_schema_removed(Rid,N,schema_state(CSin,SSIn),CSout).

%derived_process_schema_not(Rid,N,schema_state(CSin,SSin)) :-
%    derived_process_schema_removed(Rid,N,schema_state(CSin,SSin)).

%derived_process_schema_not(Rid,N,schema_state(CSout,SSout)) :-
%    derived_process_clean(Rid,N,_,_,_,schema_state(CSout,SSout),_,_,_,_).

%derived_process_schema(Rid,N-1,schema_state(CSout,SSout),PidMA,SS2MA) :- 
%    derived_process_schema(Rid,N,schema_state(CSout,SSout),PidMA,SS2MA),
    %not derived_process_clean(Rid,N,_,_,schema_state(CSout,_),schema_state(csRemoved,_),_,_,_,_),
    %not derived_process_schema_not(Rid,N-1,schema_state(CSout,SSout)),
%    not derived_process_schema_removed(Rid,N-1,schema_state(CSout,_)),
%    N>0.

derived_process_schema(Rid,N-1,schema_state(CSout,SSout),PidMA,SS2MA,CSx) :- 
    derived_process_schema(Rid,N,schema_state(CSout,SSout),PidMA,SS2MA,CSx),
    not derived_process_clean(Rid,N-1,_,_,schema_state(CSout,_),schema_state(csRemoved,_),_,_,_,_),
    not derived_process_clean(Rid,N-1,_,_,_,schema_state(CSout,_),_,_,_,_),
    % additional exclusion for one hop removed schema
    not derived_output(Rid,_,_,_,_,_,N-1,csRemoved,schema_state(CSout,SSout),_,_,_),
    %not parallel_recipe_violation(Rid,_,_,_,_,_,_,PidMA,_,_,_,SS2MA),
    %not derived_process_schema_not(Rid,N,_,CSx),
    %CSout!=csRemoved,
    %not derived_process_schema_removed(Rid,N-1,schema_state(CSout,_)),
    %not derived_process_schema_removed(Rid,M,schema_state(CSout,SSout)),
    %max_derived_output_n(Rid,MaxN),
    %M=N..MaxN,
    N>0.

derived_process_schema_not(Rid,N+1,J,CSx) :-
    derived_process_schema(Rid,N,J,PidMA,SS2MA,CSx),
    derived_process_schema_removed(Rid,M,J,CSx),    
    N<=M.

derived_process_schema_not(Rid,N,J,CSx) :-
    derived_process_schema(Rid,N,J,PidMA,SS2MA,CSx),
    parallel_recipe_violation(Rid,_,_,_,_,_,_,PidMA,_,_,_,SS2MA).

%derived_process_schema_x(Rid,N,schema_state(CSout,SSout),PidMA,SS2MA) :- 
%    derived_process_schema(Rid,N,schema_state(CSout,SSout),PidMA,SS2MA,CSx),
%    not derived_process_schema_not(Rid,N,_,CSx).

derived_process_schema_clean(Rid,N,J,PidMA,SS2MA) :- 
    derived_process_schema(Rid,N,J,PidMA,SS2MA,CSx),
    not derived_process_schema_not(Rid,N,_,CSx).
    %M=N..MaxN,
    %max_derived_output_n(Rid,MaxN).

% clean duplicate states, used the most recent one
max_derived_output_inv_n(Rid,N) :-
    derived_output_inv(Rid,_,_,_,_,_,_,_,_,_,_,_),
    N = #max{MaxN:derived_output_inv(Rid,_,_,_,_,_,MaxN,_,_,_,_,_)}.

max_derived_output_n(Rid,N) :-
    derived_output(Rid,_,_,_,_,_,_,_,_,_,_,_),
    N = #max{MaxN:derived_output(Rid,_,_,_,_,_,MaxN,_,_,_,_,_)}.    

%#show max_n/2.

derived_output_inv_not(Rid,N,Min,Mout) :- 
    max_derived_output_inv_n(Rid,NN),
    derived_output_inv(Rid,_,_,_,_,_,N,_,_,_,_,_),
    MN=N+1..NN,
    derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,MN,CSout,Min,Mout,PidM,SS2M).

%#show derived_output_inv_not/4.

derived_output_inv_clean(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M) :-
    derived_output_inv(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout,Min,Mout,PidM,SS2M),
    not derived_output_inv_not(Rid,N,Min,Mout).
    %not derived_output_inv(Rid,_,_,_,_,_,N+1,_,Min,Mout,_,_).

minimal_input(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSin) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    recipe(Rid,SS2,Pid,_),
    process_input(Pid,_,CSin),
    not derived_output(Rid,S1,S2,seq(_,SS1),M-1,Sign,_,CSin).      

minimal_input(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSin) :-
    test_serialize_recipe(Rid,A,B,S1,S2,seq(SS1,SS2),M,Sign,N),
    %M>0,
    minimal_input(Rid,S1,S2,seq(_,SS1),M-1,Sign,_,CSin).     

derived_output_temp(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout) :- 
    derived_output(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSout),
    not minimal_input(Rid,_,_,seq(SS2,_),_,Sign,_,CSout).

%#show minimal_input/8.

%minimal_input_schema_recipe(Rid,CSin,Key,Value) :-
%    minimal_input(Rid,S1,S2,seq(SS1,SS2),M,Sign,N,CSin),
%    column_schema(CSin,Key,Value).    

%derived_output_schema_recipe(Rid,CSout,Key,Value) :-
%    derived_output_temp(Rid,S1,S2,seq(SS1,SS2),M,Sign,0,CSout),
%    column_schema(CSout,Key,Value).

minimal_input_schema_recipe(Rid,CSin,Key,Value) :-
    derived_process_schema_clean(Rid,N,schema_state(CSin,_),PidMA,SS2MA),
    max_derived_output_n(Rid,N),
    column_schema(CSin,Key,Value).

derived_output_schema_recipe(Rid,CSout,Key,Value) :-
    derived_process_schema_clean(Rid,0,schema_state(CSout,_),PidMA,SS2MA),
    not parallel_recipe_violation(Rid,_,_,_,_,_,_,PidMA,_,_,_,SS2MA),
    column_schema(CSout,Key,Value).

removed_schema(Rid,SS1,Pid,CSin) :-
    recipe(Rid,SS1,Pid,_),
    process_input(Pid,_,CSin),
    not process_output(Pid,_,CSin).

#show minimal_input_schema_recipe/4.
#show derived_output_schema_recipe/4.

    
