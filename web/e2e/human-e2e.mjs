// Scripted "human" operating the REAL web UI in a real browser, for the
// end-to-end door: accept the caller's request, send one progress segment, then
// deliver a final that carries FINAL_TOKEN. No cherry LLM — the human's actions
// are the script, so the Go test can assert deterministically.
//
// Env: WEB_URL, WEB_TOKEN, FINAL_TOKEN.
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
const finalToken = need("FINAL_TOKEN");

const browser = await chromium.launch({ channel: "chrome", headless: true });
try {
  const page = await browser.newPage();
  page.on("dialog", (dialog) => dialog.accept());
  page.setDefaultTimeout(45000);

  page.on("console", (msg) => console.log("PAGE:", msg.text()));
  page.on("pageerror", (err) => console.log("PAGEERR:", String(err)));
  await page.goto(`${webURL}/?token=${webToken}`);
  console.log("url after goto:", page.url());
  for (let i = 0; i < 25; i++) {
    const snap = await page.evaluate(async () => {
      const response = await fetch("/api/state");
      const body = await response.json().catch(() => ({}));
      return { status: response.status, inbox: (body.inbox || []).length, conv: (body.conversations || []).length, msg: body.message };
    });
    console.log(`state[${i}]:`, JSON.stringify(snap));
    if (snap.inbox > 0) break;
    await page.waitForTimeout(1000);
  }

  // Accept the caller's request from the inbox.
  await page.locator('[data-testid="inbox-item"]').first().waitFor();
  await page.locator('[data-action="accept"]').first().click();
  await page.locator("#draft").waitFor({ state: "visible" });

  // One progress segment — the stream stays open, the turn stays active.
  await page.locator("#draft").fill("Looking into it — I'll roll it back the safe way.");
  await page.locator("#b-send").click();
  await page.waitForTimeout(400);

  // Deliver final: end the session with the answer the caller must receive.
  await page.locator("#draft").fill(
    `Redeploy the last known-good build, watch the health checks, then drain the bad version. ${finalToken}`,
  );
  await page.locator("#send-final").click();

  // The conversation must reach terminal (send-final disabled) in the real UI.
  await page.waitForFunction(() => {
    const button = document.getElementById("send-final");
    return button && button.disabled;
  });
  console.log("human-e2e-ok");
} finally {
  await browser.close();
}
