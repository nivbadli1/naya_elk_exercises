# Elasticsearch architecture: animated explainer

| File | What it is |
|---|---|
| `elasticsearch-explainer.mp4` | Rendered video, 1920x1080, 30 fps, 5:46, no audio |
| `elasticsearch-explainer.html` | The same animation as an interactive player. A single file that works offline. |
| `voiceover-script.txt` | Voice-over script, one paragraph per scene, with timestamps |

## Playing the HTML version
Open `elasticsearch-explainer.html` in any modern browser. It needs no internet or install.

- **Space** or a click on the picture: Play / Pause
- **Left / Right arrows** or **Prev / Next**: previous or next scene. Prev first restarts the current scene.
- **Progress bar**: click to jump within the scene. The white ticks mark the numbered steps.
- **1-9 buttons**: jump to a scene
- **"Pause at each step"**: playback stops at every step so the teacher can talk, then press Space to continue
- **Speed**: 0.5x to 1.5x

## Scenes
| # | Start | Length | Scene |
|---|---|---|---|
| 1 | 0:00 | 30 s | Index and shards |
| 2 | 0:30 | 40 s | Inside a shard (Lucene index, segments, merging, 2,147,483,519 limit) |
| 3 | 1:10 | 36 s | Primary and replica (2 nodes, failure, promotion) |
| 4 | 1:46 | 36 s | How a document finds its shard (hash(_id) % primaries) |
| 5 | 2:22 | 40 s | Full example: 3 nodes, 2 primaries, 2 replicas each, Node 1 fails |
| 6 | 3:02 | 45 s | Writing one document |
| 7 | 3:47 | 44 s | Reading one document (round-robin over copies) |
| 8 | 4:31 | 45 s | Bulk request |
| 9 | 5:16 | 30 s | Recap |

## Re-rendering the MP4
The animation is a deterministic function of time. Open the HTML with `?render=1` and call `window.__seek(seconds)` for each frame (Playwright + ffmpeg).
