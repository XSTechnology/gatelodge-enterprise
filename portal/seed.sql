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
