-- Seed the four starter tasks (idempotent by name).
INSERT INTO tasks (name, goal, success_criteria, inputs, schedule, notify) VALUES
('Nightly KB gap sweep',
 'Find questions from chats and inbound email in the last 24 hours that the knowledge base did not confidently answer. Cluster them with gap_cluster, then write ONE draft knowledge document for the largest cluster using kb_write (name it draft-<topic>). If there are no clusters, report that the knowledge base has no gaps today.',
 'Either a draft document was created for a real gap, or a clear report states there were no clusters. Do not create an empty or speculative document.',
 'Tables via db_query: chat_sessions (needs_review, escalated, last_topic), chat_messages, inbound_emails (status review). Tools: gap_cluster, kb_write, report.',
 'daily', TRUE),
('Weekly estate report',
 'Summarize the past 7 days of the Gatelodge estate using db_query: total chat sessions, answer confidence rate (chat_messages where role=assistant and confident), escalations, inbound emails answered vs review, new leads, new licenses issued. Produce a concise markdown report with the numbers.',
 'A report containing the actual numbers for all six metrics, computed from the database — not guessed.',
 'Tables via db_query: chat_sessions, chat_messages, inbound_emails, leads, licenses.',
 'weekly', TRUE),
('Trial follow-up nudge',
 'Find contacts whose trial download is older than 7 days and who have no license purchase (db_query over leads and licenses joined on email). For each qualifying contact, use email_draft to record a short, warm follow-up offering help with the trial. Do NOT send anything — drafts only.',
 'A draft recorded for each qualifying contact, or a clear report that no contacts qualify. No emails sent.',
 'Tables via db_query: leads (trial downloads with created_at), licenses (purchases). Tool: email_draft.',
 'manual', TRUE),
('Site health check',
 'Fetch each of these URLs with web_fetch and report the HTTP status of each: https://datagatelodge.com/ , https://datagatelodge.com/gatelodge.html , https://datagatelodge.com/services.html , https://datagatelodge.com/eula.html , https://get.datagatelodge.com/healthz , https://chat.datagatelodge.com/healthz . List any that are not HTTP 200.',
 'All six URLs checked with their actual statuses; failures clearly identified.',
 'The six URLs listed in the goal.',
 'daily', TRUE)
ON CONFLICT DO NOTHING;
