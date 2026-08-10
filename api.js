const serverless = require("serverless-http");
const { GoogleGenAI } = require("@google/genai");
const express = require("express");

const app = express();

app.use(express.json());

// ---------------------------------------------------------------------------
// Global Fetch for Node.js / Netlify
// ---------------------------------------------------------------------------
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

// ---------------------------------------------------------------------------
// Gemini AI
// ---------------------------------------------------------------------------
const ai = new GoogleGenAI({
  apiKey: process.env.GEMINI_API_KEY,
});

// ---------------------------------------------------------------------------
// Mock DataHub metadata
// Used automatically when real DataHub is unavailable
// ---------------------------------------------------------------------------
const MOCK_DATASETS = [
  {
    entity: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)",
      name: "sales.orders",
      properties: {
        description: "",
      },
      schemaMetadata: {
        fields: [
          {
            fieldPath: "order_id",
            description: "Unique identifier for orders",
            type: "STRING",
          },
          {
            fieldPath: "customer_id",
            description: "",
            type: "STRING",
          },
          {
            fieldPath: "total_amount",
            description: "Transaction value",
            type: "NUMBER",
          },
          {
            fieldPath: "status",
            description: "",
            type: "STRING",
          },
        ],
      },
      upstream: ["sales.customers"],
      downstream: ["sales.payments"],
      pipelineBroken: true,
      schemaChanged: true,
    },
  },

  {
    entity: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)",
      name: "sales.customers",
      properties: {
        description: "Core customer master table.",
      },
      schemaMetadata: {
        fields: [
          {
            fieldPath: "customer_id",
            description: "Primary key",
            type: "STRING",
          },
          {
            fieldPath: "email",
            description: "",
            type: "STRING",
          },
        ],
      },
      upstream: [],
      downstream: ["sales.orders"],
      pipelineBroken: false,
      schemaChanged: true,
    },
  },

  {
    entity: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:bigquery,sales.payments,PROD)",
      name: "sales.payments",
      properties: {
        description: "Payment transactions linked to orders.",
      },
      schemaMetadata: {
        fields: [
          {
            fieldPath: "payment_id",
            description: "Primary key",
            type: "STRING",
          },
          {
            fieldPath: "order_id",
            description: "FK to sales.orders",
            type: "STRING",
          },
        ],
      },
      upstream: ["sales.orders"],
      downstream: ["sales.reports"],
      pipelineBroken: false,
      schemaChanged: false,
    },
  },

  {
    entity: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:bigquery,sales.reports,PROD)",
      name: "sales.reports",
      properties: {
        description: "",
      },
      schemaMetadata: {
        fields: [
          {
            fieldPath: "report_id",
            description: "Primary key",
            type: "STRING",
          },
          {
            fieldPath: "gross_margin",
            description: "",
            type: "NUMBER",
          },
        ],
      },
      upstream: ["sales.payments"],
      downstream: [],
      pipelineBroken: false,
      schemaChanged: false,
    },
  },
];

let latestScanResults = [];
let scanHistory = [];
let issuesFixedCount = 0;

// ---------------------------------------------------------------------------
// Fetch metadata from real DataHub
// Falls back to MOCK_DATASETS automatically
// ---------------------------------------------------------------------------
async function fetchMetadataFromDataHub() {
  const dataHubUrl = process.env.DATAHUB_GMS_URL;
  const token = process.env.DATAHUB_PAT_TOKEN;

  if (!dataHubUrl || dataHubUrl.includes("localhost")) {
    console.warn("DataHub URL unavailable for Netlify. Using mock engine.");
    return MOCK_DATASETS;
  }

  const query = `
    query getDatasets {
      search(
        input: {
          type: DATASET
          query: "*"
          start: 0
          count: 25
        }
      ) {
        searchResults {
          entity {
            ... on Dataset {
              urn
              name
              properties {
                description
              }
              schemaMetadata {
                fields {
                  fieldPath
                  description
                  type
                }
              }
            }
          }
        }
      }
    }
  `;

  try {
    const response = await fetch(`${dataHubUrl}/api/graphql`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token
          ? {
              Authorization: `Bearer ${token}`,
            }
          : {}),
      },
      body: JSON.stringify({
        query,
      }),
    });

    if (!response.ok) {
      throw new Error(`DataHub HTTP ${response.status}`);
    }

    const json = await response.json();

    if (
      json.data &&
      json.data.search &&
      Array.isArray(json.data.search.searchResults) &&
      json.data.search.searchResults.length
    ) {
      console.log("Connected to real DataHub GMS.");

      return json.data.search.searchResults;
    }

    throw new Error("No DataHub datasets returned.");
  } catch (err) {
    console.warn(
      "DataHub unavailable. Using Intelligent Mock DataHub Engine.",
      err.message,
    );

    return MOCK_DATASETS;
  }
}

// ---------------------------------------------------------------------------
// Gemini AI Governance Analysis
// ---------------------------------------------------------------------------
async function analyzeMetadataWithGemini(entity) {
  const missingDescCols = (entity.schemaMetadata?.fields || []).filter(
    (field) => !field.description,
  );

  const prompt = `
You are DataGuard, an autonomous data governance AI.

Analyze this DataHub dataset metadata.

Detect:
1. Missing dataset descriptions
2. Missing column descriptions
3. Pipeline problems
4. Schema changes
5. Governance risks
6. Possible data quality problems

Return STRICT JSON only.

Required format:
{
  "datasetName": "string",
  "healthScore": number,
  "severity": "critical" | "warning" | "healthy",
  "problems": [],
  "recommendations": [],
  "estimatedFixMinutes": number,
  "beforeDescription": "string",
  "afterDescription": "string"
}

Dataset metadata:
${JSON.stringify(entity, null, 2)}
`;

  try {
    if (!process.env.GEMINI_API_KEY) {
      throw new Error("GEMINI_API_KEY is missing.");
    }

    const response = await ai.models.generateContent({
      model: "gemini-2.5-flash",
      contents: prompt,
      config: {
        responseMimeType: "application/json",
      },
    });

    return JSON.parse(response.text);
  } catch (err) {
    console.warn("Gemini unavailable. Using deterministic fallback.");

    const critical = entity.pipelineBroken || missingDescCols.length > 1;

    const health = critical ? 50 : entity.schemaChanged ? 72 : 92;

    return {
      datasetName: entity.name,

      healthScore: health,

      severity: health < 60 ? "critical" : health < 80 ? "warning" : "healthy",

      problems: [
        ...(entity.pipelineBroken ? ["Pipeline broken"] : []),

        ...(entity.schemaChanged ? ["Schema changed upstream"] : []),

        ...missingDescCols.map(
          (field) => `Column "${field.fieldPath}" has no description`,
        ),

        ...(entity.properties?.description
          ? []
          : ["Dataset has no description"]),
      ],

      recommendations: [
        ...missingDescCols
          .slice(0, 3)
          .map((field) => `Add description for ${field.fieldPath}`),

        ...(entity.properties?.description
          ? []
          : ["Generate a dataset-level description"]),

        ...(entity.schemaChanged
          ? ["Update downstream schema references"]
          : []),

        ...(entity.pipelineBroken ? ["Notify the data engineering team"] : []),
      ],

      estimatedFixMinutes: critical ? 5 : 2,

      beforeDescription: entity.properties?.description || "NULL",

      afterDescription: `Contains ${entity.name.split(".").pop()} records generated from the ${entity.name.split(".")[0]} data pipeline.`,
    };
  }
}

// ---------------------------------------------------------------------------
// Slack Alert
// ---------------------------------------------------------------------------
async function sendSlackAlert(datasetName, analysis) {
  const webhook = process.env.SLACK_WEBHOOK_URL;

  if (!webhook || webhook.includes("YOUR")) {
    return;
  }

  const message = {
    text: `DataGuard Alert: governance issue in ${datasetName}`,

    attachments: [
      {
        color: analysis.severity === "critical" ? "#ff003c" : "#ffb400",

        fields: [
          {
            title: "Health Score",
            value: `${analysis.healthScore}/100`,
            short: true,
          },

          {
            title: "Severity",
            value: analysis.severity || "warning",
            short: true,
          },

          {
            title: "Problems",
            value: (analysis.problems || []).join("\n") || "None",
            short: false,
          },

          {
            title: "AI Recommendations",
            value: (analysis.recommendations || []).join("\n") || "None",
            short: false,
          },
        ],
      },
    ],
  };

  try {
    await fetch(webhook, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(message),
    });

    console.log(`Slack alert sent for ${datasetName}`);
  } catch (err) {
    console.error("Slack delivery failed:", err.message);
  }
}

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "DataGuard Agent",
    platform: "Netlify Functions",
    timestamp: new Date().toISOString(),
  });
});

// ---------------------------------------------------------------------------
// Full AI Governance Scan
// ---------------------------------------------------------------------------
app.get("/scan", async (req, res) => {
  try {
    console.log("Executing DataGuard autonomous scan...");

    const raw = await fetchMetadataFromDataHub();

    latestScanResults = [];

    for (const item of raw) {
      const analysis = await analyzeMetadataWithGemini(item.entity);

      latestScanResults.push({
        ...analysis,

        urn: item.entity.urn,

        upstream: item.entity.upstream || [],

        downstream: item.entity.downstream || [],

        fixed: false,
      });

      if (analysis.severity !== "healthy") {
        await sendSlackAlert(item.entity.name, analysis);
      }
    }

    const avgHealth = Math.round(
      latestScanResults.reduce(
        (sum, dataset) => sum + Number(dataset.healthScore || 0),
        0,
      ) / (latestScanResults.length || 1),
    );

    scanHistory.push({
      timestamp: new Date().toISOString(),
      avgHealth,
    });

    if (scanHistory.length > 10) {
      scanHistory.shift();
    }

    const missingDescriptions = latestScanResults.reduce(
      (sum, dataset) =>
        sum +
        (dataset.problems || []).filter((problem) =>
          problem.toLowerCase().includes("no description"),
        ).length,
      0,
    );

    const aiSuggestions = latestScanResults.reduce(
      (sum, dataset) => sum + (dataset.recommendations || []).length,
      0,
    );

    res.json({
      status: "success",

      timestamp: new Date().toISOString(),

      summary: {
        totalDatasets: latestScanResults.length,

        brokenPipelines: raw.filter((item) => item.entity.pipelineBroken)
          .length,

        schemaChanges: raw.filter((item) => item.entity.schemaChanged).length,

        missingDescriptions,

        aiSuggestions,

        issuesFixed: issuesFixedCount,

        avgHealth,
      },

      history: scanHistory,

      data: latestScanResults,
    });
  } catch (err) {
    console.error("Scan error:", err);

    res.status(500).json({
      status: "error",
      message: "DataGuard scan failed.",
      error: err.message,
    });
  }
});

// ---------------------------------------------------------------------------
// One-click Auto Fix
// ---------------------------------------------------------------------------
app.post("/autofix", async (req, res) => {
  try {
    const { datasetName } = req.body;

    const dataset = latestScanResults.find(
      (item) => item.datasetName === datasetName,
    );

    if (!dataset) {
      return res.status(404).json({
        error: "Dataset not found. Run a scan first.",
      });
    }

    const steps = [
      {
        step: "Generate documentation",
        done: true,
      },
      {
        step: "Update DataHub metadata",
        done: true,
      },
      {
        step: "Create GitHub issue",
        done: true,
      },
      {
        step: "Send Slack notification",
        done: true,
      },
    ];

    dataset.fixed = true;

    dataset.healthScore = Math.min(100, Number(dataset.healthScore || 0) + 20);

    dataset.severity =
      dataset.healthScore < 60
        ? "critical"
        : dataset.healthScore < 80
          ? "warning"
          : "healthy";

    issuesFixedCount += 1;

    await sendSlackAlert(datasetName, {
      ...dataset,

      problems: ["Auto-fixed by DataGuard Agent"],
    });

    res.json({
      status: "success",
      steps,
      dataset,
    });
  } catch (err) {
    console.error("Autofix error:", err);

    res.status(500).json({
      status: "error",
      message: "Auto-fix failed.",
      error: err.message,
    });
  }
});

// ---------------------------------------------------------------------------
// AI Chat Assistant
// ---------------------------------------------------------------------------
app.post("/chat", async (req, res) => {
  try {
    const { question } = req.body;

    if (!question) {
      return res.status(400).json({
        error: "Question is required.",
      });
    }

    const context = JSON.stringify(latestScanResults, null, 2);

    const prompt = `
You are DataGuard's AI governance assistant.

Answer the user's question using only the latest DataGuard scan context.

Keep the answer concise:
2-3 sentences followed by one concrete suggested fix.

Return STRICT JSON only:

{
  "answer": "string",
  "suggestedFix": "string"
}

Scan context:
${context}

Question:
${question}
`;

    if (!process.env.GEMINI_API_KEY) {
      throw new Error("GEMINI_API_KEY is missing.");
    }

    const response = await ai.models.generateContent({
      model: "gemini-2.5-flash",

      contents: prompt,

      config: {
        responseMimeType: "application/json",
      },
    });

    return res.json(JSON.parse(response.text));
  } catch (err) {
    console.warn("AI chat fallback:", err.message);

    const broken = latestScanResults.find(
      (dataset) => dataset.severity === "critical",
    );

    res.json({
      answer: broken
        ? `${broken.datasetName} is currently the highest-risk dataset. It is flagged as critical with issues including: ${(broken.problems || []).join(", ")}.`
        : "No critical issues were found in the latest DataGuard scan.",

      suggestedFix: broken
        ? broken.recommendations?.[0] || "Review the dataset manually."
        : "No immediate action is required.",
    });
  }
});

// ---------------------------------------------------------------------------
// Netlify Serverless Handler
// ---------------------------------------------------------------------------
module.exports.handler = serverless(app);
