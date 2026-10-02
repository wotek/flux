---
layout: false
---

<script setup>
import { useRouter } from 'vitepress'

const router = useRouter()
if (typeof window !== 'undefined') {
  router.go('/')
}
</script>

<meta http-equiv="refresh" content="0; url=/">
