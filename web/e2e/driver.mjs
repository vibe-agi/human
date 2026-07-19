// Browser half of the Human web real door. Launched by the gated Go test with:
//   WEB_URL, WEB_TOKEN         — the web daemon under test
//   CHERRY_URL, CHERRY_KEY,    — OpenAI-compatible endpoint simulating the
//   CHERRY_MODEL                 human expert's judgement
//   FINAL_TOKEN                — literal marker the simulated human must include
//
// It operates the REAL UI in a real browser: log in via the one-time token
// URL, accept the request from the inbox, ask the LLM what a human expert
// would answer, type it into the composer, and deliver it as final.
import { chromium } from "playwright";

const need = (name) => {
  const value = process.env[name];
  if (!value) {
    console.error(`missing env ${name}`);
    process.exit(2);
  }
  return value;
};

const webURL = need("WEB_URL");
const webToken = need("WEB_TOKEN");
const cherryURL = need("CHERRY_URL");
const cherryKey = need("CHERRY_KEY");
const cherryModel = need("CHERRY_MODEL");
const finalToken = need("FINAL_TOKEN");

async function simulatedHumanReply(question) {
  const response = await fetch(`${cherryURL}/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${cherryKey}` },
    body: JSON.stringify({
      model: cherryModel,
      messages: [
        {
          role: "system",
          content:
            "You are simulating a human expert answering a coding agent's question through a support console. " +
            `Answer briefly (under 80 words) and you MUST include the literal token ${finalToken} somewhere in your reply.`,
        },
        { role: "user", content: question },
      ],
    }),
  });
  if (!response.ok) {
    throw new Error(`cherry api ${response.status}: ${await response.text()}`);
  }
  const body = await response.json();
  const text = body.choices?.[0]?.message?.content?.trim();
  if (!text) throw new Error("cherry api returned no content");
  return text.includes(finalToken) ? text : `${text}\n${finalToken}`;
}

const browser = await chromium.launch({ channel: "chrome", headless: true });
try {
  const page = await browser.newPage();
  page.on("dialog", (dialog) => dialog.accept());
  page.setDefaultTimeout(30000);

  await page.goto(`${webURL}/?token=${webToken}`);

  // Accept the first inbox request through the real UI.
  await page.locator('[data-testid="inbox-item"]').first().waitFor();
  const question = await page
    .locator('[data-testid="inbox-item"] .preview')
    .first()
    .innerText();
  await page.locator('[data-action="accept"]').first().click();
  await page.locator("#draft").waitFor({ state: "visible" });

  // Ask the simulated human what to answer, type it, deliver final.
  const answer = await simulatedHumanReply(question);
  await page.locator("#draft").fill(answer);
  await page.locator("#send-final").click();

  // The conversation must reach its terminal phase in the UI.
  await page.waitForFunction(() => {
    const buttons = document.getElementById("send-final");
    return buttons && buttons.disabled;
  });
  console.log("browser-door-ok");
} finally {
  await browser.close();
}
