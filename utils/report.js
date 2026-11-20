/**
 * The translation layer: turns SessionSeal's technical same-origin findings
 * into a plain-language verification report a distributor's reviewer can act
 * on — WITHOUT assuming they know audio/crypto. Every line explains, in their
 * words, what the finding means for the "is this AI?" question.
 *
 * Honest by construction: the verdict follows the band (STRONG/MODERATE/WEAK),
 * never always "strong"; only checks that actually evaluated are shown as
 * evidence; nothing here claims a human made the music — it evidences REAL
 * STUDIO WORK, which an AI export cannot produce.
 */

// Each check code -> how to phrase it when it MATCHED, and why it matters for AI.
// `meaning` is the "in reviewer's words / for the AI question" line.
const CHECK_COPY = {
  "A1": {
    title: "The stems rebuild the finished track",
    matched: "The separate instrument/vocal tracks, mixed together, reproduce this exact song.",
    meaning: "An AI-generated track is a single exported file — it has no separate stems that mix down to it. Having them is a hallmark of real production.",
    tip: "Stems are the individual layers of a song (drums, bass, vocals…). We checked they actually combine into the released master.",
  },
  "A2": {
    title: "Recorded takes are found inside the stems",
    matched: "The raw recordings in the project session appear inside the stems.",
    meaning: "The building blocks trace back to actual recording sessions, not a one-shot AI output.",
    tip: "We matched the audio clips in the producer's session file against the stems by sound.",
  },
  "A3": {
    title: "The stems are explained by the session",
    matched: "The stems can be traced back to recordings in the project session.",
    meaning: "The stems didn't appear from nowhere — they come from a real, inspectable session.",
    tip: "The reverse of the previous check: each stem contains material from the session's recordings.",
  },
  "A4": {
    title: "Session recordings are audible in the master",
    matched: "The session's recordings can be heard in the final track.",
    meaning: "The finished song is built from the recorded material in the session.",
    tip: "We matched the session's audio against the released master by sound.",
  },
  "B1": {
    title: "The session holds more than the release",
    matched: "The project contains extra material that isn't in the released song — unused takes and alternates.",
    meaning: "Real sessions are messy and contain leftovers. An AI export contains only itself, nothing extra.",
    tip: "Producers keep far more in a session than what ships — alternate takes, scrapped ideas. We detected such extra material.",
  },
  "B2": {
    title: "Edit history is present",
    matched: "The project includes saved backups from while it was being worked on.",
    meaning: "Iterative saves are evidence of a human working over time, not an instant generation.",
    tip: "Logic and other DAWs keep project backups as you work. Their presence shows an editing process happened.",
  },
  "B3": {
    title: "Recordings span multiple days",
    matched: "The recordings were made across more than one date.",
    meaning: "Real production happens over multiple sessions; AI generation is instantaneous. This is one of the hardest things to fake.",
    tip: "Recorded audio files carry the date they were captured. We found recordings from different days.",
  },
  "C1": {
    title: "Technical settings are consistent",
    matched: "The project's audio settings match a normal studio recording.",
    meaning: "The session's technical fingerprint is consistent with real recording equipment.",
    tip: "Things like sample rate — we checked they're what a real recording session would use.",
  },
  "C3": {
    title: "Session length matches the track",
    matched: "The project's length lines up with the finished song.",
    meaning: "The session and the release describe the same piece of music.",
    tip: "We compared how long the project is to how long the released track is.",
  },
  "C4": {
    title: "Declared files are present",
    matched: "The files the project references are actually present in it.",
    meaning: "The session is internally consistent — it isn't a shell pointing at missing pieces.",
    tip: "A project file lists the audio it uses; we confirmed those files are really there.",
  },
};

const BAND_VERDICT = {
  STRONG: {
    headline: "Strong evidence of real studio production",
    summary: "This track was sealed with SessionSeal along with its project session and individual stems, and multiple independent signals point to genuine human studio work — the kind of evidence an AI-generated export cannot produce.",
  },
  MODERATE: {
    headline: "Moderate evidence of real studio production",
    summary: "This track was sealed with its session and stems, and several signals point to real studio work. Some checks couldn't be evaluated (see the details), so the evidence is supportive but not comprehensive.",
  },
  WEAK: {
    headline: "Limited evidence of studio production",
    summary: "This track was sealed, but few of the studio-work signals could be confirmed. Review the details before relying on this alone.",
  },
  CONTRADICTED: {
    headline: "Evidence is inconsistent",
    summary: "The sealed material shows contradictions between the parts. This record should not be treated as evidence of coherent studio work without closer review.",
  },
};

function bandKey(band) {
  return (band || "").toUpperCase() in BAND_VERDICT ? (band || "").toUpperCase() : "WEAK";
}

/**
 * Build the reviewer-facing report from a record row (which includes the
 * sameorigin_report jsonb). Returns:
 *   verdict (headline + summary), sealed_at, artist, title,
 *   evidence[] (the plain-language matched findings for Layer 2),
 *   methodology { band, score, coherence, checks[], notes } for Layer 3.
 */
function buildReport(rec) {
  const so = rec.sameorigin_report || {};
  const band = bandKey(rec.sameorigin_band || so.band);
  const verdict = BAND_VERDICT[band];

  const checks = Array.isArray(so.checks) ? so.checks : [];
  const evidence = [];
  for (const c of checks) {
    const code = (c.check || "").slice(0, 2).trim(); // "A1 stems..." -> "A1"
    const copy = CHECK_COPY[code];
    if (!copy) continue;
    if (c.result === "match") {
      evidence.push({
        title: copy.title,
        finding: copy.matched,
        meaning: copy.meaning,
        tip: copy.tip,
        strength: "confirmed",
      });
    }
    // not_evaluable / contradiction are surfaced in methodology, not as positive evidence
  }

  return {
    title: rec.title || "Untitled",
    artist: rec.artist_name,
    sealed_at: rec.sealed_at,
    verdict: { band, ...verdict },
    evidence,
    integrity: {
      signed: !!rec.manifest_public_url,
      self_attested: rec.signer_self_attested !== false,
      cert_subject: rec.cert_subject,
      manifest_url: rec.manifest_public_url,
      note: "The record was cryptographically sealed and timestamped when "
          + "created, and can't have been altered or fabricated afterward. "
          + "The signature is currently self-attested (a CA-issued certificate "
          + "is the next step); integrity holds regardless.",
    },
    methodology: {
      band,
      score: rec.sameorigin_score,
      coherence: {
        verified: rec.coherence_verified,
        confidence: rec.coherence_confidence,
      },
      checks: checks.map((c) => ({
        check: c.check, result: c.result, weight: c.weight,
        note: c.note, value: c.value,
      })),
      notes: so.notes || [],
      red_flags: so.red_flags || [],
    },
  };
}

module.exports = { buildReport };
