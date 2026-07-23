// A headless browser acting as the human operator. The test gives it only
// ephemeral Human/relay credentials; the external LLM key remains in the host
// Go process behind REPLY_URL.
import { chromium } from "playwright";
import { writeFile } from "node:fs/promises";
import path from "node:path";

const need = (name) => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`missing environment variable ${name}`);
  return value;
};

const webURL = need("WEB_URL");
const webToken = need("WEB_TOKEN");
const replyURL = need("REPLY_URL");
const replyToken = need("REPLY_TOKEN");
const finalMarker = need("FINAL_MARKER");
const action = process.env.HUMAN_ACTION?.trim() || "final";
const toolMarker = process.env.TOOL_MARKER?.trim() || "";
const toolProfile = process.env.TOOL_PROFILE?.trim() || "codex";
const toolCommand = process.env.TOOL_COMMAND?.trim() || "";
const taskMarker = process.env.TASK_MARKER?.trim() || "";
const workspacePath = process.env.WORKSPACE_PATH?.trim() || "browser-workspace.txt";
const workspaceFirst = process.env.WORKSPACE_FIRST || "workspace-v1\n";
const workspaceSecond = process.env.WORKSPACE_SECOND || "workspace-v2\n";
const faultSyncURL = process.env.FAULT_SYNC_URL?.trim() || "";
const faultSyncToken = process.env.FAULT_SYNC_TOKEN?.trim() || "";

async function expertReply(prompt) {
  const response = await fetch(replyURL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${replyToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ prompt }),
  });
  if (!response.ok) {
    throw new Error(`host reply relay returned ${response.status}: ${await response.text()}`);
  }
  const body = await response.json();
  const answer = body.answer?.trim();
  if (!answer) throw new Error("host reply relay returned no answer");
  return answer.includes(finalMarker) ? answer : `${answer}\n${finalMarker}`;
}

const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  page.setDefaultTimeout(60_000);
  page.on("dialog", async (dialog) => {
    console.error(`browser-dialog:${dialog.type()}:${dialog.message()}`);
    await dialog.accept();
  });
  page.on("pageerror", (error) => console.error(`page-error: ${error}`));

  await page.goto(`${webURL}/?token=${encodeURIComponent(webToken)}`, {
    waitUntil: "domcontentloaded",
  });

  // The test runs one caller at a time. Do not search the preview for the
  // marker: real clients prepend enough system context that Web intentionally
  // truncates the user prompt before the marker can appear.
  const inbox = page.locator('[data-testid="inbox-item"]').first();
  await inbox.waitFor();
  const question = (await inbox.locator(".preview").innerText()).trim();

  if (action === "reject") {
    // Streaming API clients commonly retry an error that arrives after the
    // HTTP 200 headers. Reject each retry as a separate queued attempt, then
    // finish after a quiet window instead of stranding the retry in the inbox.
    let rejected = 0;
    while (rejected < 8) {
      const item = page.locator('[data-testid="inbox-item"]').first();
      if (rejected > 0) {
        try {
          await item.waitFor({ state: "visible", timeout: 12_000 });
        } catch (error) {
          if (error?.name === "TimeoutError") break;
          throw error;
        }
      }
      await item.locator('[data-action="reject"]').click();
      rejected += 1;
      await page.waitForFunction(() =>
        document.querySelectorAll('[data-testid="inbox-item"]').length === 0,
      );
    }
    if (rejected === 8) throw new Error("caller kept retrying after eight human rejections");
    console.log(`browser-human-rejected-attempts:${rejected}`);
    console.log(`browser-human-ok:reject:${finalMarker}`);
    process.exitCode = 0;
  } else {
  await inbox.locator('[data-action="accept"]').click();
  await page.locator("#draft").waitFor({ state: "visible" });
  await page.locator("#send-final").waitFor({ state: "visible" });
  await page.waitForFunction(() => !document.getElementById("send-final")?.disabled);

  // Exercise a non-terminal streaming reply before the final delivery.
  const progress = `Reviewing the request for ${finalMarker}.`;
  await page.locator("#draft").fill(progress);
  await page.locator("#b-send").click();
  await page.locator("#transcript .bubble", { hasText: progress }).waitFor();
  // Workspace scenarios have two real caller round trips. Start the real LLM
  // reply while those tools execute so a slow human simulator cannot trip the
  // caller's idle timeout after the final tool result.
  const earlyWorkspaceAnswer = action === "workspace"
    ? expertReply(`${question}\n\nThe Human will deliver the reviewed workspace changes through native file tools. Required final marker: ${finalMarker}`)
    : null;

  // Partial-SSE recovery probes hold the final response until the real caller
  // has reissued the interrupted semantic request. The synchronization URL is
  // an ephemeral loopback-only test endpoint, never part of the product API.
  if (faultSyncURL) {
    const sync = await fetch(faultSyncURL, {
      headers: faultSyncToken ? { "Authorization": `Bearer ${faultSyncToken}` } : {},
    });
    if (!sync.ok) throw new Error(`fault synchronization returned ${sync.status}: ${await sync.text()}`);
    console.log(`browser-fault-replay-observed:${finalMarker}`);
  }

  if (action === "abandon_on_caller_gone") {
    await page.locator("#abandon-btn").waitFor({ state: "visible" });
    await page.locator("#abandon-btn").click();
    await page.locator("#composer").waitFor({ state: "hidden" });
    console.log(`browser-human-ok:abandon_on_caller_gone:${finalMarker}`);
  } else {

  if (action === "workspace") {
    const relative = workspacePath;
    await page.locator("#workspace-sect").waitFor({ state: "visible" });
    const resolvedRoot = path.resolve((await page.locator("#workspace-path").inputValue()).trim());
    console.log(`browser-workspace-root:${resolvedRoot}`);
    const target = path.resolve(resolvedRoot, relative);
    if (target !== resolvedRoot && !target.startsWith(`${resolvedRoot}${path.sep}`)) {
      throw new Error("WORKSPACE_PATH must stay under the selected Human working directory");
    }
    const deliver = async (kind) => {
      const change = page.locator("#review .change", { hasText: relative }).filter({ hasText: kind }).first();
      await change.waitFor({ timeout: 30_000 });
      console.log(`browser-workspace-deliver:${kind}`);
      const resultCount = await page.locator("#transcript .tool.result pre").count();
      await page.locator("#deliver-changes").click();
      // Native file calls can finish between two browser polls, so the
      // disabled phase is not reliably observable. A newly persisted result is
      // the stable caller-continuation boundary.
      await page.waitForFunction((before) =>
        document.querySelectorAll("#transcript .tool.result pre").length > before,
      resultCount, { timeout: 30_000 });
      console.log(`browser-workspace-result:${kind}`);
      await page.waitForFunction(() => !document.getElementById("send-final")?.disabled, null, { timeout: 30_000 });
      await page.locator("#review-sect").waitFor({ state: "hidden", timeout: 30_000 });
      console.log(`browser-workspace-settled:${kind}`);
    };
    await writeFile(target, workspaceFirst, { mode: 0o600 });
    console.log("browser-workspace-written:create");
    await deliver("create");
    await writeFile(target, workspaceSecond, { mode: 0o600 });
    console.log("browser-workspace-written:modify");
    await deliver("modify");
  }

  if (action === "tasks") {
    if (!taskMarker) throw new Error("TASK_MARKER is required for tasks action");
    await page.locator("#plan-sect").waitFor({ state: "visible" });
    const native = await page.locator("#plan-native").innerText();
    console.log(`browser-task-profile:${toolProfile}:${native}`);
    const syncTask = async (suffix, button = "#send-todos") => {
      const resultCount = await page.locator("#transcript .tool.result pre").count();
      await page.locator(button).click();
      console.log(`browser-task-deliver:${toolProfile}:${suffix}:${taskMarker}`);
      // Local task tools can complete quickly enough for the disabled phase to
      // begin and end between two browser polls. The new result is the durable
      // continuation boundary; waiting on it avoids a timing-only false failure.
      await page.waitForFunction((before) =>
        document.querySelectorAll("#transcript .tool.result pre").length > before, resultCount);
      await page.waitForFunction(() => !document.getElementById("send-final")?.disabled);
      const output = await page.locator("#transcript .tool.result pre").last().innerText();
      console.log(`browser-task-result:${toolProfile}:${suffix}:${taskMarker}`);
      return output;
    };

    await page.locator("#todo-new").fill(`Verify ${taskMarker}`);
    await page.locator("#add-todo").click();
    await syncTask("pending");
    for (const status of ["in_progress", "completed"]) {
      await page.locator("#todo-list .tstate").first().click();
      await syncTask(status);
    }
    if (toolProfile === "claude") {
      const listed = await syncTask("list", "#refresh-tasks");
      if (!listed.includes(taskMarker) || !listed.toLowerCase().includes("completed")) {
        throw new Error(`Claude TaskList did not retain completed task ${taskMarker}: ${listed}`);
      }
    }
  }

  if (toolMarker) {
    const command = toolCommand || `printf ${toolMarker} > tool-proof.txt && printf ${toolMarker}`;
    await page.locator("#command-disc").waitFor({ state: "visible" });
    await page.locator("#command-disc").evaluate((element) => {
      element.closest("details").open = true;
    });
    const native = await page.locator("#command-native").innerText();
    const expectedNative = { claude: "Bash", opencode: "bash", codex: "exec_command" }[toolProfile];
    if (!expectedNative || native !== expectedNative) {
      throw new Error(`unexpected command profile ${toolProfile}:${native}`);
    }
    console.log(`browser-command-profile:${toolProfile}:${native}`);
    const resultCount = await page.locator("#transcript .tool.result pre").count();
    await page.locator("#command").fill(command);
    await page.locator("#command").press("Control+Enter");
    await page.waitForFunction((before) =>
      document.querySelectorAll("#transcript .tool.result pre").length > before, resultCount);
    await page.locator("#transcript .tool.result", { hasText: toolMarker }).waitFor();
    await page.waitForFunction(() => !document.getElementById("send-final")?.disabled);
  }

  const transcript = (await page.locator("#transcript").innerText()).trim();
  const answer = earlyWorkspaceAnswer
    ? await earlyWorkspaceAnswer
    : await expertReply(
      `${question}\n\nHuman console transcript:\n${transcript}\n\nRequired final marker: ${finalMarker}`,
    );
  await page.locator("#draft").fill(answer);
  await page.locator("#send-final").click();
  await page.waitForFunction(() =>
    document.getElementById("send-final")?.disabled &&
    document.getElementById("phasebar")?.classList.contains("term"),
  );

  console.log(`browser-human-ok:${action}:${finalMarker}`);
  }
  }
} finally {
  await browser.close();
}
