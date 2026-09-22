import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function freePort() {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test("registration, vacancy applications, private CVs, tokens, and advisor access", { timeout: 60_000 }, async (t) => {
  const workdir = await mkdtemp(path.join(root, ".workflow-test-"));
  assert.equal(path.dirname(workdir), root);
  await copyFile(path.join(root, "server.js"), path.join(workdir, "server.js"));
  await mkdir(path.join(workdir, "public"));
  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  const child = spawn(process.execPath, [path.join(workdir, "server.js")], {
    cwd: workdir,
    env: {
      ...process.env,
      PORT: String(port),
      NODE_ENV: "test",
      DATABASE_URL: "",
      DATABASE: "",
      POSTGRES_URL: "",
      POSTGRESQL_URL: "",
      LINK_ADMIN_EMAILS: "workflow-admin@example.test",
      ADMIN_EMAILS: "",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout.on("data", (chunk) => { output += chunk; });
  child.stderr.on("data", (chunk) => { output += chunk; });
  t.after(async () => {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((resolve) => child.once("exit", resolve));
    }
    assert.equal(path.dirname(workdir), root);
    await rm(workdir, { recursive: true, force: true });
  });

  let ready = false;
  for (let attempt = 0; attempt < 80; attempt += 1) {
    if (child.exitCode !== null) break;
    try {
      const response = await fetch(`${base}/health`);
      const health = await response.json();
      if (health.ok && health.storage.mode === "json") {
        ready = true;
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(ready, `Isolated server did not start: ${output}`);

  async function api(route, { token, body, status = 200, method = body ? "POST" : "GET" } = {}) {
    const response = await fetch(`${base}${route}`, {
      method,
      headers: {
        ...(token ? { authorization: `Bearer ${token}` } : {}),
        ...(body ? { "content-type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    const result = await response.json();
    assert.equal(response.status, status, `${route}: ${JSON.stringify(result)}`);
    return result;
  }

  async function register(email, accountType) {
    return api("/api/auth/register", {
      status: 201,
      body: { email, accountType, password: "test-password-123", displayName: email.split("@")[0] },
    });
  }

  const admin = await register("workflow-admin@example.test", "person");
  const company = await register("workflow-company@example.test", "company");
  const candidate = await register("workflow-candidate@example.test", "person");
  const other = await register("workflow-other@example.test", "person");
  assert.equal(admin.user.isAdmin, true);
  assert.equal(company.user.tokenBalance, 5);
  assert.equal((await api("/api/auth/login", {
    body: { email: "workflow-candidate@example.test", password: "test-password-123" },
  })).user.id, candidate.user.id);

  await api("/api/admin/state", { token: company.token, status: 403 });
  await api("/api/advisor/state", { token: other.token, status: 403 });
  await api("/api/admin/users", {
    token: admin.token,
    body: { id: other.user.id, action: "make-advisor" },
  });
  const advisor = await api("/api/advisor/state", { token: other.token });
  assert.ok(advisor.users.some((item) => item.id === company.user.id));

  await api("/api/admin/tokens/load", {
    token: admin.token,
    body: { userId: company.user.id, amount: 25, note: "workflow test" },
  });
  const vacancy = await api("/api/vacancies", {
    token: company.token,
    status: 201,
    body: { title: "Operador de sala", company: "Empresa Test", city: "Medellin" },
  });
  assert.equal(vacancy.status, "pending");
  assert.equal(vacancy.tokenBalance, 5);
  await api(`/api/vacancies/${vacancy.id}/apply`, { token: candidate.token, body: {}, status: 404 });
  await api("/api/admin/moderate", {
    token: admin.token,
    body: { type: "vacancies", id: vacancy.id, action: "publish" },
  });
  await api(`/api/vacancies/${vacancy.id}/apply`, { token: candidate.token, body: {}, status: 400 });
  await api(`/api/vacancies/${vacancy.id}/apply`, { token: company.token, body: {}, status: 400 });

  await api("/api/profile", {
    token: candidate.token,
    body: { displayName: "Candidata Test", phone: "3000000000", city: "Medellin" },
  });
  const resume = await api("/api/resumes", {
    token: candidate.token,
    status: 201,
    body: {
      fullName: "Candidata Test", headline: "Operadora", city: "Medellin",
      phone: "3000000000", documentId: "123456789", summary: "Experiencia privada",
      attachmentName: "hoja-de-vida.pdf", attachmentData: "data:application/pdf;base64,dGVzdA==",
    },
  });
  assert.equal(resume.status, "pending");
  await api(`/api/vacancies/${vacancy.id}/apply`, { token: candidate.token, body: {}, status: 400 });
  await api("/api/admin/moderate", {
    token: admin.token,
    body: { type: "resumes", id: resume.id, action: "publish" },
  });
  const application = await api(`/api/vacancies/${vacancy.id}/apply`, {
    token: candidate.token, body: {}, status: 201,
  });
  assert.equal(application.application.candidateName, "Candidata Test");

  const companyState = await api("/api/state", { token: company.token });
  const basic = companyState.vacancyApplications.find((item) => item.vacancyId === vacancy.id);
  assert.equal(basic.candidateName, "Candidata Test");
  assert.equal(basic.phone, undefined);
  assert.equal(basic.email, undefined);
  assert.equal(basic.documentId, undefined);
  const publicCv = companyState.resumes.find((item) => item.id === resume.id);
  assert.equal(publicCv.contactLocked, true);
  for (const key of ["phone", "email", "documentId", "summary", "attachmentName", "attachmentData"]) {
    assert.equal(publicCv[key], "", `${key} leaked in public CV`);
  }
  const strangerState = await api("/api/state", { token: other.token });
  assert.equal(strangerState.vacancyApplications.length, 0);
  assert.equal(strangerState.resumes.find((item) => item.id === resume.id).phone, "");
  await api(`/api/resumes/${resume.id}/download`, { token: other.token, body: {}, status: 403 });
  await api(`/api/resumes/${resume.id}/download`, { body: {}, status: 401 });
  const ownCv = await api(`/api/resumes/${resume.id}/download`, { token: candidate.token, body: {} });
  assert.match(ownCv.html, /3000000000/);
  const paidCv = await api(`/api/resumes/${resume.id}/download`, { token: company.token, body: {} });
  assert.match(paidCv.html, /3000000000/);
  assert.equal(paidCv.tokenBalance, 0);
  await api(`/api/resumes/${resume.id}/download`, { token: company.token, body: {}, status: 402 });
  assert.equal((await api("/api/me", { token: company.token })).user.tokenBalance, 0);

  await api("/api/admin/learning-materials", {
    body: { kind: "programas", title: "Programa sin admin", fileName: "demo.zip", fileData: "data:application/zip;base64,UEsDBAoAAAAA" },
    status: 401,
  });
  const program = await api("/api/admin/learning-materials", {
    token: admin.token,
    status: 201,
    body: { kind: "programas", title: "Herramienta de prueba", description: "Instalador para casinos", fileName: "herramienta.zip", fileData: "data:application/zip;base64,UEsDBAoAAAAA", tokenCost: 5, status: "published" },
  });
  assert.equal(program.status, "published");
  assert.equal(program.tokenCost, 5);
  await api("/api/admin/learning-materials", {
    token: admin.token,
    status: 400,
    body: { kind: "programas", title: "Formato rechazado", fileName: "demo.pdf", fileData: "data:application/pdf;base64,dGVzdA==" },
  });
  await api("/api/admin/learning-materials", {
    token: company.token,
    status: 403,
    body: { kind: "programas", title: "Sin permiso", fileName: "demo.zip", fileData: "data:application/zip;base64,UEsDBAoAAAAA" },
  });
  const training = await api("/api/admin/learning-materials", {
    token: admin.token,
    status: 201,
    body: { kind: "capacitaciones", title: "Induccion de prueba", fileName: "induccion.pdf", fileData: "data:application/pdf;base64,SG9sYQ==", tokenCost: 3, status: "published" },
  });
  const catalog = await api("/api/state");
  assert.deepEqual(catalog.learningMaterials.map((item) => item.id), [training.id, program.id]);
  assert.equal("mediaData" in catalog.learningMaterials[0], false);
  assert.equal("fileData" in catalog.learningMaterials[0], false);
  await api(`/api/learning-materials/${program.id}/download`, { body: {}, status: 401 });
  await api(`/api/learning-materials/${program.id}/download`, { token: candidate.token, body: {}, status: 403 });
  await api("/api/admin/tokens/load", {
    token: admin.token,
    body: { userId: company.user.id, amount: 10, note: "learning material test" },
  });
  const programDownload = await fetch(`${base}/api/learning-materials/${program.id}/download`, {
    method: "POST", headers: { authorization: `Bearer ${company.token}`, "content-type": "application/json" }, body: "{}",
  });
  assert.equal(programDownload.status, 200);
  assert.equal(programDownload.headers.get("content-type"), "application/zip");
  assert.match(programDownload.headers.get("content-disposition"), /attachment/);
  assert.equal(programDownload.headers.get("x-link-token-balance"), "5");
  assert.deepEqual(Buffer.from(await programDownload.arrayBuffer()), Buffer.from("UEsDBAoAAAAA", "base64"));
  const trainingDownload = await fetch(`${base}/api/learning-materials/${training.id}/download`, {
    method: "POST", headers: { authorization: `Bearer ${company.token}`, "content-type": "application/json" }, body: "{}",
  });
  assert.equal(trainingDownload.status, 200);
  assert.equal(trainingDownload.headers.get("content-type"), "application/pdf");
  assert.equal(trainingDownload.headers.get("x-link-token-balance"), "2");
  await api(`/api/learning-materials/${program.id}/download`, { token: company.token, body: {}, status: 402 });
  assert.equal((await api("/api/me", { token: company.token })).user.tokenBalance, 2);
  const learningAdminState = await api("/api/admin/state", { token: admin.token });
  assert.ok(learningAdminState.content.learningMaterials.some((item) => item.id === program.id));
  assert.ok(learningAdminState.tokenTransactions.some((item) => item.kind === "learning_download" && item.referenceId === training.id));

  const imageData = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lXcAAAAASUVORK5CYII=";
  const lowAd = await api("/api/admin/ad-campaigns", {
    token: admin.token,
    status: 201,
    body: { title: "Pauta baja", priority: 1, status: "published", mediaData: imageData, mediaType: "image/png", mediaName: "pauta.png" },
  });
  const highAd = await api("/api/admin/ad-campaigns", {
    token: admin.token,
    status: 201,
    body: { title: "Pauta alta", priority: 10, status: "published", mediaData: imageData, mediaType: "image/png", mediaName: "pauta.png" },
  });
  const ads = (await api("/api/state")).activeAds;
  assert.deepEqual(ads.map((item) => item.id), [highAd.id, lowAd.id]);
  for (const ad of ads) {
    assert.match(ad.mediaUrl, /^\/api\/media-files\//);
    const response = await fetch(`${base}${ad.mediaUrl}`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "image/png");
    assert.ok((await response.arrayBuffer()).byteLength > 0);
  }
  await api("/api/admin/moderate", {
    token: admin.token,
    body: { type: "adCampaigns", id: highAd.id, action: "hide" },
  });
  assert.deepEqual((await api("/api/state")).activeAds.map((item) => item.id), [lowAd.id]);
  assert.equal((await fetch(`${base}${highAd.mediaUrl}`)).status, 404);
});
