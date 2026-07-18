---------------------------- MODULE HumanCommon ----------------------------
(***************************************************************************)
(* Protocol-neutral definitions shared by the Human runtime, HumanLLM, and  *)
(* HumanAgent models.  This module deliberately contains no lifecycle state: *)
(* a completion response and an A2A Task do not have interchangeable states. *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

LLMKind   == "llm"
AgentKind == "agent"
Surfaces  == {LLMKind, AgentKind}

ScopedKey(kind, scope, id) == <<kind, scope, id>>
KeyKind(key)  == key[1]
KeyScope(key) == key[2]
KeyID(key)    == key[3]

IsScopedKey(key, kinds, scopes, ids) ==
  /\ key \in kinds \X scopes \X ids
  /\ KeyKind(key) \in Surfaces

SeqRange(sequence) == {sequence[index] : index \in DOMAIN sequence}

RemoveFromSeq(sequence, value) ==
  SelectSeq(sequence, LAMBDA element: element # value)

LLMResponseTerminalStates == {"text_final", "tool_calls", "clarification", "error"}
AgentTerminalStates == {"completed", "canceled", "rejected", "failed"}

IsLLMResponseTerminal(state) == state \in LLMResponseTerminalStates
IsAgentTerminal(state) == state \in AgentTerminalStates

=============================================================================
