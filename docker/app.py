import os
import re
from google.cloud import bigquery
import google.auth

from slack_bolt import App, Ack
from slack_bolt.adapter.socket_mode import SocketModeHandler

APP_ENVIRONMENT = os.environ.get("APP_ENVIRONMENT", "")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN", "")
PORT = os.environ.get("PORT", 8080)
app = App(
    token=SLACK_BOT_TOKEN,
    process_before_response=True
)


def bigquery_create_table():
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    client = bigquery.Client(credentials=credentials, project="project-id")
    query = """
CREATE OR REPLACE TABLE `project-id.llm_dataset.embedded_data` AS (
SELECT * FROM
  ML.GENERATE_TEXT_EMBEDDING( MODEL `project-id.llm_dataset.embedding`,
  (
  SELECT
    content
  FROM
    `project-id.AppSheetDatabases.app-sheet`
  ),
  STRUCT(TRUE AS flatten_json_output)
  )
);
"""
    answer = []
    try:
        query_job = client.query(query)  # Make an API request.
        answer = [row["answer"] for row in query_job]
    except Exception as e:
        return str(e)

    if answer:
        return "".join(answer)
    else:
        return "データ更新完了"


def bigquery_lm_query(prompt):
    # Construct a BigQuery client object.
    client = bigquery.Client(location="asia-northeast1", project="project-id")

    query = f"""
DECLARE question_text STRING
DEFAULT \"{prompt}\";

WITH embedded_question AS (
  SELECT
    *
  FROM
    ML.GENERATE_TEXT_EMBEDDING( MODEL `project-id.llm_dataset.embedding`,
      (SELECT question_text AS content),
      STRUCT(TRUE AS flatten_json_output))
),
embedded_faq AS (
  SELECT
    *
  FROM
  `project-id.llm_dataset.embedded_data` WHERE content is not null
),

search_result AS (
  SELECT
    q.content as question,
    f.content as reference,
    ML.DISTANCE(q.text_embedding, f.text_embedding, 'COSINE') AS vector_distance
  FROM
    `embedded_question` AS q,
    `embedded_faq` AS f
  ORDER BY
    vector_distance
  LIMIT 1
)

, prompt_text AS (
SELECT
  CONCAT(
    '以下の参考情報を踏まえて、質問文に対して回答してください。参照元をつけてください。わからない場合は無理に回答せず「わかりません」と回答してください。',
    ' 質問文：', question,
    ' 参考情報:', reference
  ) AS prompt
FROM
  search_result
)
SELECT
  STRING(ml_generate_text_result.predictions[0].content) AS answer
FROM
  ML.GENERATE_TEXT( MODEL `project-id.llm_dataset.remote_llm`,
    (SELECT * FROM `prompt_text`),
    STRUCT(
      0.2 AS temperature,
      1000 AS max_output_tokens)
  );
"""
    answer = []
    try:
        query_job = client.query(query)  # Make an API request.
        answer = [row["answer"] for row in query_job]
    except Exception as e:
        return str(e)

    if answer:
        return "".join(answer)


def handle_mention(event, say):
    query = str(re.sub("<@.*>", "", event['text']))
    query = query.lstrip("\n")
    query = query.replace("\n", "\\n")

    thread_id = event['ts']
    if "thread_ts" in event:
        thread_id = event['thread_ts']

    say('問い合わせ対応中', thread_ts=thread_id)

    if len(query) < 2:
        answer = bigquery_create_table()
    else:
        answer = bigquery_lm_query(query)

    say(answer, thread_ts=thread_id)


def slack_ack(ack: Ack):
    ack()


app.event("app_mention")(ack=slack_ack, lazy=[handle_mention])


# アプリを起動します
if __name__ == "__main__":
    if APP_ENVIRONMENT == "prod":
        app.start(port=int(PORT))
    else:
        print("SocketModeHandler")
        SocketModeHandler(app, SLACK_APP_TOKEN).start()
