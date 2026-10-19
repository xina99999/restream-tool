const state = {
  selectedJobId: null,
  selectedVideoId: null,
  restartJob: null,
  frameJob: null,
  frameTimer: null,
  videos: [],
  downloads: [],
  completedDownloadIds: new Set(),
  uploading: false,
  showRawLogs: false,
  refreshTimer: null,
  bootRetryTimer: null,
  toastTimer: null,
};

const $ = (id) => document.getElementById(id);
const UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024;

async function api(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail || detail;
    } catch (_) {
      // keep default
    }
    throw new Error(detail);
  }
  return response.json();
}

function formatDuration(seconds) {
  if (!seconds) return "unknown";
  const rounded = Math.round(seconds);
  const h = String(Math.floor(rounded / 3600)).padStart(2, "0");
  const m = String(Math.floor((rounded % 3600) / 60)).padStart(2, "0");
  const s = String(rounded % 60).padStart(2, "0");
  return `${h}:${m}:${s}`;
}

function formatSize(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setFormStatus(message, type = "info") {
  const status = $("formStatus");
  status.textContent = message || "";
  status.className = `form-status ${type}`;
}

function notify(message, type = "info") {
  const toast = $("toast");
  toast.textContent = message || "";
  toast.className = `toast show ${type}`;
  if (state.toastTimer) {
    clearTimeout(state.toastTimer);
  }
  state.toastTimer = setTimeout(() => {
    toast.className = "toast";
  }, type === "error" ? 7000 : 3200);
}

function selectedVideo() {
  return state.videos.find((video) => video.id === state.selectedVideoId) || null;
}

function sortVideos(videos) {
  const mode = $("videoSort").value;
  const timeOf = (video) => new Date(video.modified_at || video.created_at);
  return [...videos].sort((a, b) => {
    if (mode === "oldest") return timeOf(a) - timeOf(b);
    if (mode === "name") return a.original_name.localeCompare(b.original_name);
    if (mode === "duration") return Number(b.duration_seconds || 0) - Number(a.duration_seconds || 0);
    return timeOf(b) - timeOf(a);
  });
}

function renderSelectedVideo() {
  const video = selectedVideo();
  $("selectedVideo").textContent = video
    ? `Selected: ${video.original_name} | ${formatDuration(video.duration_seconds)} | ${formatSize(video.size_bytes)}`
    : "No video selected";
}

function syncEncodingProfile() {
  const isH264 = $("encodingProfile").value === "h264_low_latency";
  $("h264Config").hidden = !isH264;
  if (isH264) {
    $("loop").checked = true;
    $("loop").disabled = true;
  } else {
    $("loop").disabled = false;
  }
}

function syncS3Source() {
  const enabled = $("useS3Source").checked;
  $("s3SourceField").hidden = !enabled;
  $("s3CacheConfig").hidden = !enabled;
  $("s3Source").required = enabled;
}

function renderVideos() {
  const query = $("videoSearch").value.trim().toLowerCase();
  const filtered = state.videos.filter((video) => video.original_name.toLowerCase().includes(query));
  const sorted = sortVideos(filtered);
  const list = $("videoList");
  list.innerHTML = "";
  $("videoCount").textContent = `${state.videos.length} videos`;
  for (const video of sorted) {
    const button = document.createElement("button");
    button.className = `video-row ${video.id === state.selectedVideoId ? "selected" : ""}`;
    button.type = "button";
    button.dataset.video = video.id;
    button.innerHTML = `
      <span class="video-name">${video.original_name}</span>
      <span class="video-meta">${formatDuration(video.duration_seconds)} | ${formatSize(video.size_bytes)}</span>
    `;
    list.appendChild(button);
  }
  renderSelectedVideo();
}

function downloadStatus(download) {
  const downloaded = Number(download.downloaded_bytes || 0);
  const total = Number(download.total_bytes || 0);
  if (download.status === "DOWNLOADING") {
    const percent = total > 0 ? ` ${Math.min(100, (downloaded / total) * 100).toFixed(1)}%` : "";
    return `${formatSize(downloaded)}${total > 0 ? ` / ${formatSize(total)}` : ""}${percent}`;
  }
  if (download.status === "FAILED") return download.error || "Download failed";
  if (download.status === "COMPLETED") return `${formatSize(downloaded)} ready`;
  return download.status === "PROCESSING" ? "Scanning video metadata..." : "Waiting to start...";
}

function renderDownloads() {
  const list = $("downloadList");
  const downloads = state.downloads.filter((item) => ["PENDING", "DOWNLOADING", "PROCESSING", "FAILED"].includes(item.status));
  list.hidden = downloads.length === 0;
  list.innerHTML = downloads.map((download) => `
    <div class="download-row">
      <span class="download-name">${escapeHtml(download.original_name)}</span>
      <div class="download-actions">
        <span class="download-status">${escapeHtml(downloadStatus(download))}</span>
        ${download.status === "FAILED" ? `<button class="secondary compact" data-resume-download="${download.id}" type="button">Retry</button>` : ""}
      </div>
    </div>
  `).join("");
}

async function loadDownloads() {
  state.downloads = await api("/api/downloads");
  const hasNewCompleted = state.downloads
    .filter((download) => download.status === "COMPLETED")
    .some((download) => !state.completedDownloadIds.has(download.id));
  state.downloads
    .filter((download) => download.status === "COMPLETED")
    .forEach((download) => state.completedDownloadIds.add(download.id));
  renderDownloads();
  return hasNewCompleted;
}

async function loadConfig() {
  const config = await api("/api/config");
  $("username").value = config.username || "";
  $("password").value = config.password || "";
  $("publishHost").value = config.publish_host || "localhost";
  $("publishPort").value = config.publish_port || 18554;
  $("viewHost").value = config.view_host || "localhost";
  $("viewPort").value = config.view_port || 18554;
  $("h264Crf").value = config.h264_defaults?.crf ?? 23;
  $("h264Preset").value = config.h264_defaults?.preset || "veryfast";
  $("h264Gop").value = config.h264_defaults?.gop ?? 30;
  syncEncodingProfile();
  syncS3Source();
}

async function loadVideos() {
  const videos = await api("/api/videos");
  state.videos = videos;
  if (!state.selectedVideoId && videos.length > 0) {
    state.selectedVideoId = videos[0].id;
  }
  if (state.selectedVideoId && !videos.some((video) => video.id === state.selectedVideoId)) {
    state.selectedVideoId = videos[0]?.id || null;
  }
  renderVideos();
}

function jobRow(job) {
  const isLoop = Boolean(job.loop);
  const percent = Number(job.progress_percent || 0).toFixed(1);
  const tr = document.createElement("tr");
  const encodedViewUrl = encodeURIComponent(job.view_url || "");
  const canStop = ["RUNNING", "STOPPING", "PENDING"].includes(job.status);
  const canResume = !canStop && Number(job.out_time_seconds || 0) > 0 && Number(job.progress_percent || 0) < 100;
  const autoActionLabel = job.auto_retry ? "Auto Off" : "Auto Complete";
  const autoActionClass = job.auto_retry ? "auto-off" : "auto-on";
  const cameraLabel = escapeHtml(job.camera_name || job.video?.original_name || job.camera_code || job.rtsp_path);
  const videoLabel = escapeHtml(job.video?.original_name || "Unknown video");
  const rtspPath = escapeHtml(job.rtsp_path);
  const status = escapeHtml(job.status);
  const durationSeconds = job.duration_seconds || job.video?.duration_seconds;
  const download = state.downloads.find((item) => item.id === job.download_id);
  const downloaded = Number(download?.downloaded_bytes || 0);
  const downloadTotal = Number(download?.total_bytes || 0);
  const downloadPercent = downloadTotal > 0 ? Math.min(100, downloaded / downloadTotal * 100) : 0;
  const cacheBuffer = Number(job.cache_buffer_seconds || 0);
  const cacheDuration = Number(job.cache_duration_seconds || 0);
  const cacheInfo = job.cache_local
    ? `<div class="cache-progress"><span>Download ${download ? `${downloadPercent.toFixed(1)}%` : "queued"}</span><span>Cache ${formatDuration(cacheDuration)} / ${formatDuration(cacheBuffer)}</span></div>`
    : "";
  tr.innerHTML = `
    <td><span class="status ${status}">${status}</span></td>
    <td>
      <div class="progress"><span style="width:${Math.min(100, Number(job.progress_percent || 0))}%"></span></div>
      ${isLoop ? "Looping" : `${percent}%`}
      <div class="progress-time">${formatDuration(job.out_time_seconds)}${isLoop ? " elapsed" : ` / ${formatDuration(durationSeconds)}`}</div>
      ${cacheInfo}
    </td>
    <td>
      <div class="camera-name">${cameraLabel}</div>
      <div class="camera-path">${rtspPath}</div>
      <div class="job-video">${videoLabel}</div>
    </td>
    <td>
      <div class="rtsp-actions">
        <button class="link-button" data-open-url="${encodedViewUrl}" type="button">View URL</button>
        <button class="copy" data-copy="${encodedViewUrl}" type="button">Copy URL</button>
      </div>
    </td>
    <td>
      <div class="action-grid">
        <button class="secondary" data-detail="${job.id}" type="button">Details</button>
        <button class="secondary" data-log="${job.id}" type="button">Logs</button>
        <button class="frame-action" data-frame="${job.id}" type="button">Frame</button>
        <button class="danger" data-stop="${job.id}" type="button" ${canStop ? "" : "disabled"}>Stop</button>
        <button class="resume" data-resume="${job.id}" type="button" ${canResume ? "" : "disabled"}>Resume</button>
        <button class="event-action" data-events="${job.id}" type="button">Events</button>
        <button class="${autoActionClass} wide" data-auto-retry="${job.id}" type="button">${autoActionLabel}</button>
        <button class="again wide" data-restart="${job.id}" type="button">Run Again</button>
        <button class="delete-action" data-delete="${job.id}" type="button">Delete</button>
      </div>
    </td>
  `;
  return tr;
}

async function loadJobs() {
  const jobs = await api("/api/jobs");
  const body = $("jobsBody");
  body.innerHTML = "";
  for (const job of jobs) {
    body.appendChild(jobRow(job));
  }
  if (!state.selectedJobId && jobs.length > 0) {
    state.selectedJobId = jobs[0].id;
  }
  if (state.selectedJobId) {
    await loadLogs(state.selectedJobId);
  }
}

async function loadLogs(jobId) {
  state.selectedJobId = jobId;
  $("activeJob").textContent = jobId;
  $("logTitle").textContent = jobId;
  const logs = await api(`/api/jobs/${jobId}/logs?limit=400&include_progress=${state.showRawLogs}`);
  $("logs").textContent = logs.map((line) => `${line.created_at} ${line.level} ${line.message}`).join("\n");
}

async function openJobDetails(jobId) {
  const [job, logs] = await Promise.all([
    api(`/api/jobs/${jobId}`),
    api(`/api/jobs/${jobId}/logs?limit=80`),
  ]);
  const video = job.video || {};
  $("jobDetailBody").innerHTML = `
    <div><strong>Job ID</strong><span>${escapeHtml(job.id)}</span></div>
    <div><strong>Status</strong><span>${escapeHtml(job.status)}</span></div>
    <div><strong>Video</strong><span>${escapeHtml(video.original_name || "-")}</span></div>
    <div><strong>Source</strong><span>${job.source_type === "s3" ? "S3 / remote URL" : escapeHtml(video.path || "-")}</span></div>
    <div><strong>Cache</strong><span>${job.cache_local ? `${escapeHtml(job.cache_status || "QUEUED")} | ${formatDuration(job.cache_duration_seconds)} / ${formatDuration(job.cache_buffer_seconds)}` : "not enabled"}</span></div>
    <div><strong>Camera</strong><span>${escapeHtml(job.camera_name || job.camera_code || "-")}</span></div>
    <div><strong>Camera code</strong><span>${escapeHtml(job.rtsp_path)}</span></div>
    <div><strong>Progress</strong><span>${Number(job.progress_percent || 0).toFixed(1)}%</span></div>
    <div><strong>Out time</strong><span>${formatDuration(job.out_time_seconds)}</span></div>
    <div><strong>Duration</strong><span>${formatDuration(job.duration_seconds || video.duration_seconds)}</span></div>
    <div><strong>Created</strong><span>${formatDateTime(job.created_at)}</span></div>
    <div><strong>Started</strong><span>${formatDateTime(job.started_at)}</span></div>
    <div><strong>Ended</strong><span>${formatDateTime(job.ended_at)}</span></div>
    <div><strong>Exit code</strong><span>${job.exit_code ?? "-"}</span></div>
    <div><strong>Transport</strong><span>${escapeHtml(job.rtsp_transport || "tcp")}</span></div>
    <div><strong>Profile</strong><span>${escapeHtml(job.encoding_profile || "copy")}</span></div>
    <div><strong>H.264 config</strong><span>${job.encoding_profile === "h264_low_latency" ? `CRF ${job.h264_crf}, ${escapeHtml(job.h264_preset)}, GOP ${job.h264_gop}, no audio` : "-"}</span></div>
    <div><strong>Loop</strong><span>${job.loop ? "yes" : "no"}</span></div>
    <div><strong>Auto resume</strong><span>${job.auto_retry ? "yes" : "no"}</span></div>
    <div><strong>Retry count</strong><span>${job.retry_count ?? 0}</span></div>
    <div><strong>Max retries</strong><span>${job.max_retries ? job.max_retries : "unlimited"}</span></div>
    <div><strong>Last error</strong><span>${escapeHtml(job.last_error || "-")}</span></div>
  `;
  $("jobDetailLogs").textContent = logs.map((line) => `${line.created_at} ${line.level} ${line.message}`).join("\n");
  $("jobDetailDialog").showModal();
}

async function openEvents(jobId) {
  const events = await api(`/api/jobs/${jobId}/events?limit=300`);
  $("eventList").innerHTML = events.length
    ? events.map((event) => `
        <div class="event-row">
          <div class="event-time">${escapeHtml(formatDateTime(event.created_at))}</div>
          <div class="event-action-name">${escapeHtml(event.action)}</div>
          <div class="event-meta">
            out: ${escapeHtml(formatDuration(event.out_time_seconds))} |
            progress: ${Number(event.progress_percent || 0).toFixed(1)}% |
            status: ${escapeHtml(event.status || "-")}
          </div>
          <div class="event-message">${escapeHtml(event.message)}</div>
        </div>
      `).join("")
    : `<div class="event-row">No events yet.</div>`;
  $("eventDialog").showModal();
}

function stopFrameRefresh() {
  if (state.frameTimer) {
    clearInterval(state.frameTimer);
    state.frameTimer = null;
  }
}

async function refreshFrame() {
  if (!state.frameJob) return;
  const jobId = state.frameJob.id;
  $("frameStatus").textContent = "Capturing frame...";
  $("frameImage").src = `/api/jobs/${jobId}/frame?t=${Date.now()}`;
}

async function openFrame(jobId) {
  const job = await api(`/api/jobs/${jobId}`);
  state.frameJob = job;
  const label = job.video?.original_name || job.camera_name || job.rtsp_path || job.id;
  $("frameJobName").textContent = `${label} | ${formatDuration(job.out_time_seconds)}`;
  $("frameImage").removeAttribute("src");
  $("frameStatus").textContent = "";
  $("frameDialog").showModal();
  await refreshFrame();
  stopFrameRefresh();
  if ($("autoRefreshFrame").checked) {
    state.frameTimer = setInterval(refreshFrame, 5000);
  }
}

function setRestartOffset(seconds) {
  const job = state.restartJob;
  const duration = Number(job?.duration_seconds || job?.video?.duration_seconds || 0);
  const bounded = Math.max(0, Math.min(Number(seconds || 0), duration || Number.MAX_SAFE_INTEGER));
  $("restartOffsetSlider").value = Math.round(bounded);
  $("restartOffsetInput").value = Math.round(bounded);
  $("restartOffsetLabel").textContent = `${formatDuration(bounded)} / ${formatDuration(duration)}`;
}

async function openRestartDialog(jobId) {
  const job = await api(`/api/jobs/${jobId}`);
  state.restartJob = job;
  const duration = Number(job.duration_seconds || job.video?.duration_seconds || 0);
  const current = Number(job.out_time_seconds || 0);
  $("restartVideoName").textContent = `${job.video?.original_name || job.camera_name || job.rtsp_path}`;
  $("restartOffsetSlider").max = Math.max(0, Math.floor(duration));
  $("restartOffsetInput").max = Math.max(0, Math.floor(duration));
  setRestartOffset(current);
  $("restartDialog").showModal();
}

async function uploadFiles(fileList) {
  const files = [...fileList];
  if (files.length === 0) return;
  if (state.uploading) {
    notify("Upload is already running.", "error");
    return;
  }
  const totalBytes = files.reduce((total, file) => total + file.size, 0);
  let completedBytes = 0;
  const items = [];
  setUploadBusy(true);
  try {
    for (const file of files) {
      items.push(await uploadFileInChunks(file, completedBytes, totalBytes));
      completedBytes += file.size;
    }
    await loadVideos();
    if (items[0]?.video_id || items[0]?.id) {
      state.selectedVideoId = items[0].video_id || items[0].id;
      renderVideos();
    }
    notify(`Uploaded ${files.length} video(s).`, "success");
  } finally {
    setUploadBusy(false);
    $("uploadProgress").hidden = true;
  }
}

function setUploadProgress(percent, text) {
  $("uploadProgress").hidden = false;
  $("uploadProgressFill").style.width = `${Math.max(0, Math.min(100, percent))}%`;
  $("uploadProgressPercent").textContent = `${Math.round(percent)}%`;
  $("uploadProgressText").textContent = text;
}

function setUploadBusy(isBusy) {
  state.uploading = isBusy;
  document.querySelectorAll(".file-action").forEach((label) => {
    label.classList.toggle("disabled", isBusy);
  });
  $("scanVideos").disabled = isBusy;
}

async function uploadFileInChunks(file, completedBytes, totalBytes) {
  const originalName = file.webkitRelativePath || file.name;
  const clientKey = `${originalName}|${file.size}|${file.lastModified}`;
  let session = await api("/api/uploads/init", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ original_name: originalName, total_bytes: file.size, client_key: clientKey }),
  });
  let offset = Number(session.received_bytes || 0);
  if (session.status === "COMPLETED") return session;

  while (offset < file.size) {
    const chunk = await file.slice(offset, offset + UPLOAD_CHUNK_BYTES).arrayBuffer();
    session = await api(`/api/uploads/${session.id}/chunk?offset=${offset}`, {
      method: "PUT",
      headers: { "Content-Type": "application/octet-stream" },
      body: chunk,
    });
    offset = Number(session.received_bytes || 0);
    const totalCompleted = completedBytes + offset;
    setUploadProgress(
      totalBytes ? (totalCompleted / totalBytes) * 100 : 100,
      `Uploading ${file.name}: ${formatSize(offset)} / ${formatSize(file.size)}`,
    );
  }
  setUploadProgress(
    totalBytes ? ((completedBytes + file.size) / totalBytes) * 100 : 100,
    `Finalizing ${file.name}...`,
  );
  return api(`/api/uploads/${session.id}/complete`, { method: "POST" });
}

$("videoFiles").addEventListener("change", async (event) => {
  try {
    await uploadFiles(event.target.files);
  } catch (error) {
    notify(error.message, "error");
  } finally {
    event.target.value = "";
  }
});

$("videoFolder").addEventListener("change", async (event) => {
  try {
    await uploadFiles(event.target.files);
  } catch (error) {
    notify(error.message, "error");
  } finally {
    event.target.value = "";
  }
});

$("scanVideos").addEventListener("click", async () => {
  try {
    const result = await api("/api/videos/scan", { method: "POST" });
    await loadVideos();
    notify(
      `Scan done. Imported ${result.imported || 0}, merged ${result.deduplicated || 0}, removed ${result.pruned || 0}.`,
      "success",
    );
  } catch (error) {
    notify(error.message, "error");
  }
});

$("startS3Download").addEventListener("click", async () => {
  const source = $("s3DownloadSource").value.trim();
  if (!source) {
    notify("Enter an S3 URI or public HTTPS URL.", "error");
    return;
  }
  const button = $("startS3Download");
  button.disabled = true;
  try {
    const download = await api("/api/downloads", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source }),
    });
    $("s3DownloadSource").value = "";
    notify(download.status === "PENDING" ? "S3 download started." : "S3 download is already running.", "success");
    await loadDownloads();
  } catch (error) {
    notify(error.message, "error");
  } finally {
    button.disabled = false;
  }
});

$("downloadList").addEventListener("click", async (event) => {
  const downloadId = event.target.dataset.resumeDownload;
  if (!downloadId) return;
  try {
    await api(`/api/downloads/${downloadId}/resume`, { method: "POST" });
    notify("S3 download resumed.", "success");
    await loadDownloads();
  } catch (error) {
    notify(error.message, "error");
  }
});

$("videoSearch").addEventListener("input", renderVideos);
$("videoSort").addEventListener("change", renderVideos);
$("encodingProfile").addEventListener("change", syncEncodingProfile);
$("useS3Source").addEventListener("change", syncS3Source);

$("videoList").addEventListener("click", (event) => {
  const row = event.target.closest("[data-video]");
  if (!row) return;
  state.selectedVideoId = row.dataset.video;
  renderVideos();
});

$("jobForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const startButton = $("startRestream");
  if (startButton.disabled) return;
  const useS3Source = $("useS3Source").checked;
  if (!useS3Source && !state.selectedVideoId) {
    setFormStatus("Select a video first.", "error");
    return;
  }
  const payload = {
    video_id: useS3Source ? null : state.selectedVideoId,
    s3_source: useS3Source ? $("s3Source").value.trim() : null,
    cache_local: useS3Source && $("cacheLocal").checked,
    cache_buffer_seconds: Number($("cacheBufferSeconds").value),
    camera_name: $("cameraName").value || null,
    rtsp_path: $("rtspPath").value,
    username: $("username").value || null,
    password: $("password").value || null,
    publish_host: $("publishHost").value,
    publish_port: Number($("publishPort").value),
    view_host: $("viewHost").value,
    view_port: Number($("viewPort").value),
    loop: $("loop").checked,
    auto_retry: $("autoRetry").checked,
    encoding_profile: $("encodingProfile").value,
    h264_crf: Number($("h264Crf").value),
    h264_preset: $("h264Preset").value,
    h264_gop: Number($("h264Gop").value),
  };
  startButton.disabled = true;
  startButton.textContent = "Starting...";
  setFormStatus("Starting restream...", "info");
  try {
    const job = await api("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.selectedJobId = job.id;
    setFormStatus(`Started job ${job.id}`, "success");
    notify("Restream started.", "success");
    await loadJobs();
  } catch (error) {
    setFormStatus(error.message, "error");
    notify(error.message, "error");
  } finally {
    startButton.disabled = false;
    startButton.textContent = "Start Restream";
  }
});

$("jobsBody").addEventListener("click", async (event) => {
  try {
    const logId = event.target.dataset.log;
    const stopId = event.target.dataset.stop;
    const copyUrl = event.target.dataset.copy;
    const openUrl = event.target.dataset.openUrl;
    const restartId = event.target.dataset.restart;
    const resumeId = event.target.dataset.resume;
    const autoRetryId = event.target.dataset.autoRetry;
    const eventsId = event.target.dataset.events;
    const deleteId = event.target.dataset.delete;
    const detailId = event.target.dataset.detail;
    const frameId = event.target.dataset.frame;
    if (eventsId) {
      await openEvents(eventsId);
    }
    if (detailId) {
      await openJobDetails(detailId);
    }
    if (frameId) {
      await openFrame(frameId);
    }
    if (logId) {
      await loadLogs(logId);
    }
    if (copyUrl) {
      await navigator.clipboard.writeText(decodeURIComponent(copyUrl));
      event.target.textContent = "Copied";
      setTimeout(() => {
        event.target.textContent = "Copy URL";
      }, 1200);
    }
    if (openUrl) {
      $("rtspDialogUrl").value = decodeURIComponent(openUrl);
      $("rtspDialog").showModal();
    }
    if (stopId) {
      await api(`/api/jobs/${stopId}/stop`, { method: "POST" });
      notify("Restream stopped.", "success");
      await loadJobs();
    }
    if (resumeId) {
      const job = await api(`/api/jobs/${resumeId}/resume`, { method: "POST" });
      state.selectedJobId = job.id;
      notify("Resume requested.", "success");
      await loadJobs();
    }
    if (autoRetryId) {
      const job = await api(`/api/jobs/${autoRetryId}/auto-retry`, { method: "POST" });
      state.selectedJobId = job.id;
      notify(job.auto_retry ? "Auto complete enabled." : "Auto complete disabled.", "success");
      await loadJobs();
    }
    if (restartId) {
      await openRestartDialog(restartId);
    }
    if (deleteId) {
      if (!confirm("Delete this restream session and its job row? Logs/events stay in the database for audit.")) {
        return;
      }
      await api(`/api/jobs/${deleteId}`, { method: "DELETE" });
      if (state.selectedJobId === deleteId) {
        state.selectedJobId = null;
        $("logs").textContent = "";
        $("logTitle").textContent = "";
        $("activeJob").textContent = "";
      }
      notify("Session deleted.", "success");
      await loadJobs();
    }
  } catch (error) {
    notify(error.message, "error");
  }
});

async function refreshJobsSafely() {
  try {
    const [, hasNewCompleted] = await Promise.all([loadJobs(), loadDownloads()]);
    if (hasNewCompleted) {
      await loadVideos();
    }
  } catch (error) {
    notify("Connection to the tool was interrupted. Retrying...", "error");
  }
}

$("refreshJobs").addEventListener("click", refreshJobsSafely);
$("toggleRawLogs").addEventListener("click", async () => {
  state.showRawLogs = !state.showRawLogs;
  $("toggleRawLogs").textContent = state.showRawLogs ? "Hide Raw" : "Show Raw";
  if (state.selectedJobId) {
    await loadLogs(state.selectedJobId);
  }
});
$("closeRtspDialog").addEventListener("click", () => $("rtspDialog").close());
$("closeJobDetailDialog").addEventListener("click", () => $("jobDetailDialog").close());
$("closeEventDialog").addEventListener("click", () => $("eventDialog").close());
$("closeFrameDialog").addEventListener("click", () => {
  stopFrameRefresh();
  $("frameDialog").close();
});
$("closeRestartDialog").addEventListener("click", () => $("restartDialog").close());
$("refreshFrame").addEventListener("click", refreshFrame);
$("autoRefreshFrame").addEventListener("change", () => {
  stopFrameRefresh();
  if ($("autoRefreshFrame").checked && $("frameDialog").open) {
    state.frameTimer = setInterval(refreshFrame, 5000);
  }
});
$("frameImage").addEventListener("load", () => {
  $("frameStatus").textContent = `Updated ${new Date().toLocaleTimeString()}`;
});
$("frameImage").addEventListener("error", () => {
  $("frameStatus").textContent = "Cannot capture frame for this job. Check Events/Logs.";
});
$("restartOffsetSlider").addEventListener("input", (event) => setRestartOffset(event.target.value));
$("restartOffsetInput").addEventListener("input", (event) => setRestartOffset(event.target.value));
$("restartFromStart").addEventListener("click", () => setRestartOffset(0));
$("restartFromCurrent").addEventListener("click", () => setRestartOffset(state.restartJob?.out_time_seconds || 0));
$("confirmRestart").addEventListener("click", async () => {
  if (!state.restartJob) return;
  const offset = Number($("restartOffsetInput").value || 0);
  const job = await api(`/api/jobs/${state.restartJob.id}/restart`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ start_offset_seconds: offset }),
  });
  state.selectedJobId = job.id;
  $("restartDialog").close();
  await loadJobs();
});
$("copyDialogUrl").addEventListener("click", async () => {
  await navigator.clipboard.writeText($("rtspDialogUrl").value);
  $("copyDialogUrl").textContent = "Copied";
  setTimeout(() => {
    $("copyDialogUrl").textContent = "Copy URL";
  }, 1200);
});

document.querySelectorAll("[data-toggle-panel]").forEach((button) => {
  button.addEventListener("click", () => {
    const panel = $(button.dataset.togglePanel);
    panel.classList.toggle("collapsed");
    button.textContent = panel.classList.contains("collapsed") ? "Expand" : "Collapse";
  });
});

async function boot() {
  try {
    await loadConfig();
    await loadVideos();
    await loadDownloads();
    await loadJobs();
    if (!state.refreshTimer) {
      state.refreshTimer = setInterval(refreshJobsSafely, 2500);
    }
  } catch (error) {
    notify("Connecting to the tool... retrying shortly.", "error");
    clearTimeout(state.bootRetryTimer);
    state.bootRetryTimer = setTimeout(boot, 1500);
  }
}

boot();
