-- Seed templates + demo client (idempotent).
INSERT INTO portal_templates (name, description, icon, goal_template, criteria_template, parameters, schedule) VALUES
('Website health monitor',
 'The agent checks your URLs and reports their HTTP status. Any failures are flagged clearly.',
 '🌐',
 'Fetch each of these URLs with web_fetch and report the HTTP status of each: {urls}. List any that are not HTTP 200 with their actual status.',
 'All provided URLs checked with their actual statuses; failures clearly identified.',
 '[{"name": "urls", "label": "URLs to check (one per line)", "type": "textarea", "placeholder": "https://example.com"}]',
 'daily'),
('Content watch brief',
 'The agent fetches your page and summarizes what it finds — useful for keeping an eye on a competitor page, a status page, or your own content.',
 '👁',
 'Fetch this URL with web_fetch and summarize what the page currently says, focusing on: {focus}. Report the key points concisely.',
 'A clear summary of the page content relevant to the focus area.',
 '[{"name": "url", "label": "URL to watch", "type": "text", "placeholder": "https://example.com/page"}, {"name": "focus", "label": "What to look for", "type": "text", "placeholder": "pricing changes, new features, announcements"}]',
 'daily'),
('Research brief',
 'The agent fetches the URLs you provide and synthesizes a brief on the topic.',
 '🔬',
 'Fetch each of these URLs with web_fetch and write a concise research brief on: {topic}. Cite which URL each point comes from.',
 'A structured brief drawing on the actual content of the provided URLs.',
 '[{"name": "topic", "label": "Topic", "type": "text", "placeholder": "AI gateway pricing trends"}, {"name": "urls", "label": "Source URLs (one per line)", "type": "textarea"}]',
 'manual'),
('Custom task',
 'Write your own goal — the agent plans, uses its capabilities, and a separate review grades the outcome.',
 '⚙',
 '{goal}',
 '{criteria}',
 '[{"name": "goal", "label": "What should the agent accomplish?", "type": "textarea", "placeholder": "Describe the outcome you want..."}, {"name": "criteria", "label": "How do you judge success? (optional)", "type": "textarea"}]',
 'manual')
ON CONFLICT DO NOTHING;

INSERT INTO portal_clients (name, company, email, access_key, brand_name)
VALUES ('Demo Client', 'Acme Corp', 'demo@acme.example',
        'portal-demo-key-2026', 'Acme Task Desk')
ON CONFLICT (access_key) DO NOTHING;

-- ── decision policy pilot: deterministic refund-eligibility ruleset ─────
-- The agent EXTRACTS facts; this ruleset DECIDES. JDM decision table,
-- first-match: abuse guard → tier rules → deny fallback.
INSERT INTO portal_rulesets (name, description, version, jdm)
SELECT 'Refund eligibility policy',
       'Deterministic refund decision: abuse guard, tier SLAs, amount/day cutoffs.',
       1,
       '{"edges": [{"id": "e1", "sourceId": "n-input", "targetId": "n-table"}, {"id": "e2", "sourceId": "n-table", "targetId": "n-output"}], "nodes": [{"id": "n-input", "type": "inputNode", "name": "Facts"}, {"id": "n-output", "type": "outputNode", "name": "Decision"}, {"id": "n-table", "type": "decisionTableNode", "name": "Refund eligibility", "content": {"hitPolicy": "first", "inputs": [{"field": "prior_refunds_90d", "id": "c1", "name": "Prior refunds 90d", "type": "expression"}, {"field": "customer_tier", "id": "c2", "name": "Tier", "type": "expression"}, {"field": "days_since_purchase", "id": "c3", "name": "Days since purchase", "type": "expression"}, {"field": "order_amount_usd", "id": "c4", "name": "Order amount", "type": "expression"}, {"field": "reason", "id": "c5", "name": "Reason", "type": "expression"}], "outputs": [{"field": "decision", "id": "o1", "name": "Decision", "type": "expression"}, {"field": "rule", "id": "o2", "name": "Rule fired", "type": "expression"}], "rules": [{"_id": "guard-abuse", "c1": ">= 3", "c2": "", "c3": "", "c4": "", "c5": "", "o1": "\"manual_review\"", "o2": "\"guard-abuse\""}, {"_id": "ent-90d", "c1": "", "c2": "== \"enterprise\"", "c3": "<= 90", "c4": "", "c5": "", "o1": "\"approved\"", "o2": "\"ent-90d\""}, {"_id": "std-defective", "c1": "", "c2": "== \"standard\"", "c3": "<= 30", "c4": "<= 500", "c5": "== \"defective\"", "o1": "\"approved\"", "o2": "\"std-defective\""}, {"_id": "premium-mindchange", "c1": "", "c2": "== \"premium\"", "c3": "<= 30", "c4": "<= 200", "c5": "== \"changed_mind\"", "o1": "\"approved\"", "o2": "\"premium-mindchange\""}, {"_id": "std-late", "c1": "", "c2": "== \"standard\"", "c3": "<= 14", "c4": "", "c5": "== \"late_delivery\"", "o1": "\"approved\"", "o2": "\"std-late\""}, {"_id": "fallback-deny", "c1": "", "c2": "", "c3": "", "c4": "", "c5": "", "o1": "\"denied\"", "o2": "\"fallback-deny\""}]}}]}'
WHERE NOT EXISTS (SELECT 1 FROM portal_rulesets
                  WHERE name = 'Refund eligibility policy');

INSERT INTO portal_templates (name, description, icon, goal_template,
                              criteria_template, parameters, schedule,
                              ruleset_id)
SELECT 'Refund eligibility decision',
       'The agent reads the refund request and extracts the facts; a deterministic, versioned policy decides. You see the rule that fired.',
       '⚖',
       'A refund request has arrived. Read the REQUEST TEXT, extract these facts, and submit them with submit_facts as ONE JSON object: customer_tier (one of "standard", "premium", "enterprise"), days_since_purchase (number), order_amount_usd (number), reason (one of "defective", "late_delivery", "changed_mind", "duplicate_charge"), prior_refunds_90d (number, default 0 if not stated). Do NOT decide the outcome yourself. Then report: list each fact you extracted with its value. The policy decision will be appended by the rules engine. REQUEST TEXT: {request_text}',
       'All five facts extracted from the request text and submitted via submit_facts; the facts accurately reflect the request.',
       '[{"name": "request_text", "label": "Refund request / case notes", "type": "textarea", "placeholder": "Paste the customer''s refund request or support case notes..."}]',
       'manual',
       (SELECT id FROM portal_rulesets WHERE name = 'Refund eligibility policy')
WHERE NOT EXISTS (SELECT 1 FROM portal_templates
                  WHERE name = 'Refund eligibility decision');
