<script setup lang="ts">
import DefaultTheme from 'vitepress/theme'
const { Layout } = DefaultTheme
</script>

<template>
  <Layout>
    <template #home-hero-info-before>
      <p class="hero-eyebrow">Servus! 👋</p>
    </template>
  </Layout>
</template>

<style scoped>
.hero-eyebrow {
  font-family: var(--vp-font-family-heading);
  font-size: 1.05rem;
  font-weight: 600;
  color: var(--vp-c-brand-1);
  margin-bottom: 0.75rem;
  letter-spacing: 0.02em;
}
</style>
