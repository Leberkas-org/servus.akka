<script setup lang="ts">
import DefaultTheme from 'vitepress/theme'
const { Layout } = DefaultTheme
</script>

<template>
  <Layout>

    <template #home-hero-after>
      <div class="alps-decoration" aria-hidden="true">
        <svg class="alps-svg" viewBox="0 0 1440 160" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
          <!-- Hintergebirge — distant misty peaks -->
          <path class="alps-back"
            d="M0,160 L0,90 L60,95 L120,60 L180,85 L240,40 L300,70 L360,25 L420,60
               L480,35 L540,55 L600,20 L660,50 L720,15 L780,45 L840,25 L900,55
               L960,35 L1020,60 L1080,40 L1140,65 L1200,50 L1260,70 L1320,55
               L1380,80 L1440,70 L1440,160 Z" />
          <!-- Mittelgebirge — mid-range ridgeline -->
          <path class="alps-mid"
            d="M0,160 L0,110 L60,105 L120,85 L200,100 L280,70 L340,90 L420,60
               L500,80 L560,55 L640,75 L720,45 L800,70 L860,50 L940,75 L1020,60
               L1100,80 L1180,65 L1260,85 L1340,75 L1440,90 L1440,160 Z" />
          <!-- Vordergebirge — closest range, matches page background -->
          <path class="alps-front"
            d="M0,160 L0,130 L80,125 L160,115 L240,125 L320,105 L400,120 L480,100
               L560,115 L640,95 L720,85 L800,105 L880,90 L960,110 L1040,95
               L1120,110 L1200,100 L1280,115 L1360,105 L1440,120 L1440,160 Z" />
        </svg>
      </div>
    </template>
  </Layout>
</template>

<style scoped>
.hero-eyebrow {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  animation: fadeInDown 0.6s ease-out both;
}

.eyebrow-text {
  font-family: var(--vp-font-family-heading);
  font-size: 1.15rem;
  font-weight: 700;
  color: var(--loden);
  letter-spacing: 0.04em;
}

.eyebrow-diamond {
  color: var(--bavaria-blue-deep);
  font-size: 0.55rem;
  opacity: 0.5;
}

.dark .eyebrow-diamond {
  color: var(--bavaria-blue-light);
}

.dark .eyebrow-text {
  color: var(--loden);
}

/* ── Alpine Mountain Silhouette ── */
.alps-decoration {
  position: relative;
  margin-top: -60px;
  z-index: 2;
  pointer-events: none;
  animation: mountainRise 1.2s ease-out 0.3s both;
}

.alps-svg {
  display: block;
  width: 100%;
  height: 120px;
}

.alps-back { fill: var(--alps-back); }
.alps-mid  { fill: var(--alps-mid); }
.alps-front { fill: var(--alps-front); }

@keyframes fadeInDown {
  from { opacity: 0; transform: translateY(-12px); }
  to   { opacity: 1; transform: translateY(0); }
}

@keyframes mountainRise {
  from { opacity: 0; transform: translateY(30px); }
  to   { opacity: 1; transform: translateY(0); }
}

@media (prefers-reduced-motion: reduce) {
  .hero-eyebrow,
  .alps-decoration {
    animation: none;
  }
}
</style>
