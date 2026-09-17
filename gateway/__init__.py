"""LLM Gateway — standalone OpenAI-compatible endpoint.

Own project, own process, own port (default 8010). Fronts the local
embedded llama.cpp engine and, from Stage 3 on, any OpenAI-compatible
cloud. Run with ``python -m gateway``.

Design doc: plans/2026-09-16_llm_gateway_v1_design.md (the model/GPU
machinery in gateway/models.py was lifted from the assistant project's
agent/embedded_llm.py — copied, not imported).
"""
