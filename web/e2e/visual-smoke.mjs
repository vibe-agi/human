import { mkdir, readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const here = dirname(fileURLToPath(import.meta.url));
const html = await readFile(join(here, "..", "static", "index.html"), "utf8");
const outputDir = process.env.VISUAL_OUTPUT_DIR || "/tmp/human-web-visual";
await mkdir(outputDir, { recursive: true });

const workspaceID = "session-32793fad9390e83e8e78";
const state = {
  schema_version: 3,
  inbox: [],
  conversations: [{
    key: {
      caller: "local-caller",
      task_id: "task_9c130985a94f90b92b57b468e65b5c82_with_a_deliberately_long_suffix",
    },
    phase: "active",
    updated_at: new Date().toISOString(),
    human_workspace: {
      id: workspaceID,
      path: "/Users/human/code/checkout/human",
      available: true,
    },
    command_profile: { tool_name: "bash", command_field: "command" },
    plan_profile: {
      kind: "list",
      tool_name: "todowrite",
      items_field: "todos",
      content_field: "content",
      status_field: "status",
      priority_field: "priority",
      default_priority: "medium",
    },
    transcript: [
      { author: "caller", kind: "text", text: "Review the workspace and make the requested change." },
      { author: "human", kind: "progress", text: "I’m checking the relevant files now." },
      {
        author: "human",
        kind: "tool_calls",
        tool_calls: [{ id: "call-1", name: "bash", input: { command: "ls -la" } }],
      },
      {
        author: "caller",
        kind: "tool_result",
        tool_call_id: "call-1",
        text: "Command completed successfully.",
      },
    ],
  }],
  review: {
    generation: 1,
    changes: [{
      id: "change-1",
      workspace_id: workspaceID,
      path: "internal/example/long-but-project-relative-file-name.go",
      kind: "modify",
      diff: "-before\n+after\n",
    }],
  },
};

const encodedState = JSON.stringify(state);
const server = createServer((request, response) => {
  const path = new URL(request.url || "/", "http://human.test").pathname;
  if (path === "/api/state") {
    response.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    response.end(encodedState);
    return;
  }
  if (path === "/api/events") {
    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-store",
      Connection: "keep-alive",
    });
    response.write(`event: state\ndata: ${encodedState}\n\n`);
    return;
  }
  if (path.startsWith("/api/")) {
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end('{"ok":true}');
    return;
  }
  response.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  response.end(html);
});

await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(0, "127.0.0.1", resolve);
});
const address = server.address();
const baseURL = `http://127.0.0.1:${address.port}`;

const browser = await chromium.launch({ channel: "chrome", headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: 1600, height: 1000 },
    colorScheme: "light",
    reducedMotion: "reduce",
  });
  const page = await context.newPage();
  await page.addInitScript(() => {
    localStorage.setItem("human-web-locale", "en");
    if (!localStorage.getItem("human-web-theme")) {
      localStorage.setItem("human-web-theme", "light");
    }
  });
  await page.goto(baseURL);
  await page.locator(".conv.sel").waitFor();

  const desktopLayout = await page.evaluate(() => {
    const card = document.querySelector(".conv.sel").getBoundingClientRect();
    const status = document.querySelector(".conv.sel .st").getBoundingClientRect();
    const id = document.querySelector(".conv.sel .id").getBoundingClientRect();
    const workspaceInput = document.getElementById("workspace-path").getBoundingClientRect();
    const workspaceButton = document.getElementById("set-workspace").getBoundingClientRect();
    const sectionHead = document.querySelector("#workspace-sect > .eyebrow").getBoundingClientRect();
    const buns = document.getElementById("buns").getBoundingClientRect();
    const connection = document.getElementById("status").getBoundingClientRect();
    return {
      documentOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
      cardOverflow: document.querySelector(".conv.sel").scrollWidth - document.querySelector(".conv.sel").clientWidth,
      idBeforeStatus: id.right <= status.left - 3,
      headerStatusAligned: Math.abs(buns.top - connection.top) <= 1 &&
        Math.abs(buns.bottom - connection.bottom) <= 1,
      controlsAligned: Math.abs(workspaceInput.top - workspaceButton.top) <= 1 &&
        Math.abs(workspaceInput.bottom - workspaceButton.bottom) <= 1,
      headingContained: sectionHead.right <= document.querySelector("#workspace-sect").getBoundingClientRect().right,
      cardContained: card.right <= document.querySelector(".dispatch").getBoundingClientRect().right,
    };
  });
  if (desktopLayout.documentOverflow > 0 || desktopLayout.cardOverflow > 0 ||
      !desktopLayout.idBeforeStatus || !desktopLayout.headerStatusAligned ||
      !desktopLayout.controlsAligned ||
      !desktopLayout.headingContained || !desktopLayout.cardContained) {
    throw new Error(`desktop layout regression: ${JSON.stringify(desktopLayout)}`);
  }
  await page.screenshot({ path: join(outputDir, "desktop-light.png"), fullPage: true });

  await page.locator("#theme").click();
  await page.waitForTimeout(120);
  const darkControls = await page.evaluate(() => {
    const button = document.getElementById("set-workspace");
    return {
      disabled: button.disabled,
      theme: document.documentElement.getAttribute("data-theme"),
      savedTheme: localStorage.getItem("human-web-theme"),
      background: getComputedStyle(button).backgroundColor,
      buttonPanel: getComputedStyle(button).getPropertyValue("--panel").trim(),
      panel: getComputedStyle(document.documentElement).getPropertyValue("--panel").trim(),
    };
  });
  if (darkControls.disabled || darkControls.theme !== "dark" ||
      darkControls.savedTheme !== "dark" ||
      darkControls.background !== "rgb(37, 37, 38)") {
    throw new Error(`dark control regression: ${JSON.stringify(darkControls)}`);
  }
  await page.screenshot({ path: join(outputDir, "desktop-dark.png"), fullPage: true });

  await page.reload();
  await page.locator(".conv.sel").waitFor();
  const restoredTheme = await page.evaluate(() => ({
    theme: document.documentElement.getAttribute("data-theme"),
    savedTheme: localStorage.getItem("human-web-theme"),
    background: getComputedStyle(document.body).backgroundColor,
  }));
  if (restoredTheme.theme !== "dark" || restoredTheme.savedTheme !== "dark" ||
      restoredTheme.background !== "rgb(30, 30, 30)") {
    throw new Error(`theme persistence regression: ${JSON.stringify(restoredTheme)}`);
  }
  await context.close();

  const narrow = await browser.newContext({
    viewport: { width: 390, height: 844 },
    colorScheme: "light",
    reducedMotion: "reduce",
  });
  const narrowPage = await narrow.newPage();
  await narrowPage.addInitScript(() => {
    localStorage.setItem("human-web-locale", "en");
    localStorage.setItem("human-web-theme", "light");
  });
  await narrowPage.goto(baseURL);
  await narrowPage.locator(".conv").waitFor({ state: "attached" });
  await narrowPage.locator('[data-view="console"]').click();
  await narrowPage.locator(".console.active").waitFor();
  const narrowLayout = await narrowPage.evaluate(() => ({
    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    offenders: [...document.querySelectorAll("body *")]
      .filter((element) => {
        const box = element.getBoundingClientRect();
        return box.right > document.documentElement.clientWidth + 1 || box.left < -1;
      })
      .slice(0, 12)
      .map((element) => ({
        tag: element.tagName,
        id: element.id,
        className: String(element.className),
        left: Math.round(element.getBoundingClientRect().left),
        right: Math.round(element.getBoundingClientRect().right),
      })),
  }));
  if (narrowLayout.overflow > 0) {
    throw new Error(`narrow layout regression: ${JSON.stringify(narrowLayout)}`);
  }
  await narrowPage.screenshot({ path: join(outputDir, "narrow-console.png"), fullPage: true });
  await narrow.close();

  console.log(`human-web-visual-ok ${outputDir}`);
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
