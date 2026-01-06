# vivek/working-0.1 vs vivek/created_from_cicd
"""
🧠 Key Difference Summary
Feature	Version 1	Version 2
Trigger type	HTTP	GCS Event
request.get_json()	✅	❌
event.data	❌	✅
Cloud Functions Gen2	❌	✅
Eventarc compatible	❌	✅
Your deployment	❌ WRONG	✅ CORRECT
"""

"""
Great question 👍
You’re basically comparing HTTP-style Cloud Function code vs Event-driven (GCS/Eventarc) Cloud Function code. Let’s break this very clearly, because only one of these is correct for your deployment.

🔴 Version 1 (❌ NOT correct for your setup)
def load_data_bigquery(request):
    event_data = request.get_json()

What this version assumes

Function is HTTP-triggered

Google sends a JSON HTTP request body

You deploy using:

--trigger-http

Why this fails for your case

You deployed using:

--trigger-event-filters=type=google.cloud.storage.object.v1.finalized
--trigger-event-filters=bucket=src-bkt-17122025


That means:

❌ This is NOT an HTTP function

❌ request.get_json() does NOT exist

❌ GCS does NOT send HTTP JSON payloads

That’s why you saw errors like:

TypeError: takes 1 positional argument but 2 were given

'dict' object has no attribute 'data'

👉 Conclusion:
❌ This version is invalid for Gen2 GCS triggers

🟢 Version 2 (✅ CORRECT for your setup)
def load_data_bigquery(event, context=None):

What this version supports

✅ Cloud Functions Gen2

✅ Eventarc

✅ GCS finalized events

✅ Both CloudEvent and legacy dict

Why this works

GCS events can arrive as:

CloudEvent → event.data

dict payload → event

Your defensive code handles both:

if hasattr(event, "data"):
    event_data = event.data
else:
    event_data = event

Your sample payload
{
  "name": "transactions.csv",
  "bucket": "src-bkt-17122025"
}


This is exactly what Eventarc sends ✔
So this code correctly extracts:

bucket_name = event_data["bucket"]
file_name = event_data["name"]

🧠 Key Difference Summary
Feature	Version 1	Version 2
Trigger type	HTTP	GCS Event
request.get_json()	✅	❌
event.data	❌	✅
Cloud Functions Gen2	❌	✅
Eventarc compatible	❌	✅
Your deployment	❌ WRONG	✅ CORRECT
✅ Final Answer (Very Important)
👉 Use ONLY this version
def load_data_bigquery(event, context=None):

👉 DO NOT use
def load_data_bigquery(request):

🔐 Bonus: Production best practice

Add a safety guard (you already mostly did):

if file_name.endswith("/"):
    return "Skipping folder"


And optionally log event ID:

print("Event received for:", file_name)

🎯 Final takeaway

HTTP functions and Event-driven functions are fundamentally different.
GCS Gen2 = Eventarc = event, NOT request.

If you want next, I can:

Convert this into Composer → GCS → CF → BQ

Add dead-letter handling

Add schema enforcement

Explain why Gen2 sends dict vs CloudEvent

Just say the word 🚀
"""