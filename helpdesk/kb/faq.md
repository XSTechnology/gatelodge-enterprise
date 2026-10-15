# FAQ — questions at the gate

title: FAQ — frequently asked questions about the trial, data privacy, self-hosting, providers, license keys, and support
description: Direct answers to the questions buyers and security teams ask most: what happens after the 30-day trial, whether data passes through DataGateLodge servers, self-hosting effort, supported providers, what a license key contains, trying before buying, and how to get support

## What happens when the 30-day trial ends?

Local inference keeps working — free, forever. Cloud serving pauses
until you enter a key: a one-time US$99 Community key, or an
enterprise license. The check runs locally; nothing phones home.

## Does our data pass through DataGateLodge servers?

No. Gatelodge is self-hosted — requests go from your gateway directly
to the providers you configured. There is no DataGateLodge SaaS;
nothing of yours transits our infrastructure.

## How much work is self-hosting, really?

One binary, one command, Windows or Linux. The admin console opens at
http://localhost:8010, provider presets add backends in a click, and
your apps point at the endpoint they already speak (OpenAI-compatible).

## Which models and providers does it support?

Ten backend types — embedded GGUF, llama-server, RunPod (pods and
serverless), OpenAI, Anthropic, Gemini, OpenAI Responses, Azure OpenAI,
Vertex AI, and Bedrock — all behind one OpenAI-compatible endpoint,
swappable per task with alias chains.

## What's actually in a license key?

A signed set of claims: edition, expiry, entitled providers, and
feature flags. Keys never display secrets, hot-reload without a
restart, and are revocable through a tamper-evident revocation list.

## Can we try it before talking to anyone?

Yes. The Community binary is the full product on a 30-day trial —
download it from the Download page at datagatelodge.com. Or ask this
agent: it runs through Gatelodge itself.

## How do we get support?

support@datagatelodge.com, the chat widget on datagatelodge.com, or
enterprise support with your license. We answer within a business day.
