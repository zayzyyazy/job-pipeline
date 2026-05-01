from typing import Dict, List, Optional

TARGET_SOURCES = {"indeed": "Indeed", "linkedin": "LinkedIn"}
JOB_TERMS = [
    "indeed",
    "linkedin",
    "job",
    "jobs",
    "hiring",
    "opportunity",
    "application",
    "recruiter",
]


def detect_source(sender: str, subject: str, snippet: str, body: str) -> Optional[str]:
    text = f"{sender} {subject} {snippet} {body}".lower()
    for key, source in TARGET_SOURCES.items():
        if key in text:
            return source
    return None


def evaluate_email_filter(email: Dict[str, str]) -> Dict[str, object]:
    sender = email.get("sender", "")
    subject = email.get("subject", "")
    snippet = email.get("snippet", "")
    body = email.get("body", "")

    source = detect_source(sender, subject, snippet, body)
    fields = {
        "sender": sender.lower(),
        "subject": subject.lower(),
        "snippet": snippet.lower(),
        "body": body.lower(),
    }

    matched_terms: List[str] = []
    matched_fields: List[str] = []
    for term in JOB_TERMS:
        for field_name, text in fields.items():
            if term in text:
                matched_terms.append(term)
                if field_name not in matched_fields:
                    matched_fields.append(field_name)
                break

    if source:
        reason = f"matched source={source}; terms={sorted(set(matched_terms)) or ['(none)']}; fields={matched_fields or ['(none)']}"
        return {
            "passed": True,
            "source": source,
            "reason": reason,
            "matched_terms": sorted(set(matched_terms)),
            "matched_fields": matched_fields,
        }

    if matched_terms:
        reason = f"no explicit source, but job terms found: {sorted(set(matched_terms))} in fields={matched_fields}"
        return {
            "passed": True,
            "source": "Other Job Source",
            "reason": reason,
            "matched_terms": sorted(set(matched_terms)),
            "matched_fields": matched_fields,
        }

    return {
        "passed": False,
        "source": None,
        "reason": "no target source and no job terms in sender/subject/snippet/body",
        "matched_terms": [],
        "matched_fields": [],
    }


def is_job_related_email(email: Dict[str, str]) -> Optional[str]:
    result = evaluate_email_filter(email)
    return str(result["source"]) if result["passed"] else None
