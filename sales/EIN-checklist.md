# EIN Application Checklist — XS Technologies Inc.

**Goal:** Federal EIN for the Delaware corporation, required for the US bank
account (Mercury/Relay) and the US Stripe account. No lawyer needed — this is
a one-page IRS form. Field names below follow **Form SS-4 (Rev. 12-2023)**;
fill by field label, not line number.

## Have on hand before starting

- [ ] Certificate of Incorporation — exact legal name + state + **date of incorporation** + **number of authorized shares** (needed for the franchise-tax question in the accountant briefing, not for SS-4)
- [ ] Registered agent's name + Delaware street address
- [ ] Owner's full legal name, home address, passport
- [ ] Whether the corporation ever applied for an EIN before (if yes, approximate date)

## SS-4 fields — recommended entries

| Field (form order) | Entry |
|---|---|
| Legal name of entity | `XS TECHNOLOGIES INC.` — exactly as on the certificate |
| Trade name / DBA | `DataGateLodge` (the public-facing division brand) |
| Care-of / third party | Leave blank (you are your own applicant) |
| Mailing address | **Owner's real home address, Canada** — IRS mail must reach you; matches later bank/Stripe KYC |
| Street address (if different) | Same as mailing |
| County and state where principal business is located | Owner's city, `Canada` (honest: the corporation is managed from there; foreign is acceptable per the instructions) |
| Responsible party — name | [OWNER FULL LEGAL NAME] (sole director/officer) |
| Responsible party — TIN (SSN/ITIN/EIN) | **`Foreign`** — you have no SSN/ITIN and are not eligible; the instructions permit this |
| Type of entity | **Corporation** → enter `1120` |
| State/country of incorporation | `Delaware` |
| Reason for applying | `Started a new business` — specify: `software sales` |
| Date business started | [DATE OF INCORPORATION from the certificate] |
| Closing month of accounting year | `December` (calendar year — matches the Delaware annual-report default) |
| Number of employees expected (next 12 months) | `0` (no US payroll planned) |
| Applied for an EIN before? | Usually `No` — answer truthfully |
| First date wages paid or expected | `N/A` |
| Principal activity | `Software company — sells licenses to AI gateway software online` |
| Principal product/service | `Gatelodge — LLM request routing/governance software (perpetual licenses)` |
| Signature | Owner signs as President/Director; date |

## Submission — pick one (online is NOT available: the responsible party has no SSN/ITIN)

| Route | Number / address | Turnaround |
|---|---|---|
| **Phone (recommended)** | **+1 267-941-1099**, Mon–Fri 6:00–11:00 ET | **EIN issued during the call** |
| Fax | 855-641-6935 (include a return fax number — they fax the EIN back) | ~4 business days |
| Mail | Per the SS-4 instructions for your address (international: Attn: EIN International Operation, Cincinnati OH 45999) | 4–6 weeks — avoid |

Phone script note: say you are applying for an EIN for a **Delaware
corporation** whose principal place of business is in **Canada**, responsible
party is a Canadian resident with no SSN/ITIN ("Foreign"). They will walk the
SS-4 with you. Write the EIN on your SS-4 copy and keep it.

## After the EIN arrives

1. [ ] Keep the CP-575 letter (or the number + date issued) — Mercury and Stripe both ask for it
2. [ ] Apply for the **Mercury** (or Relay) business account: certificate + EIN letter + owner passport; fully remote, ~1–2 weeks
3. [ ] Open the **US Stripe account** as XS Technologies Inc. (owner as representative with Canadian tax ID + passport — supported per Stripe's rep/owner tax-ID requirements)
4. [ ] Book the accountant consult (see `accountant-briefing.md`)
5. [ ] Calendar two annual dates NOW: **Delaware annual report + franchise tax — March 1** · **US 1120 + Form 5472 — April 15** (foreign-owned corporation files 5472 even with zero revenue; the penalty for missing it is $25,000)

*Placeholders in [BRACKETS] are owner-filled. This checklist is operational
guidance, not legal or tax advice — the accountant briefing is the companion
document.*
