//! Design tokens / theming for the `Canvas.*` DOM-built components
//! (`document.rs`, `vector_search.rs`, `agent_monitor.rs`). The browser
//! demo (`examples/dashboard/`) mounts `apply_theme` once per page to inject
//! a `:root { --tpt-* }` block (see `css_variables`); components then
//! reference `var(--tpt-*)` in their inline styles so a single theme switch
//! restyles every component. `Theme` itself is a plain struct with no
//! `web-sys` dependency, so it is host-testable via `cargo test`.

/// A theme is just a named palette. Two presets ship: `default_theme`
/// (light) and `dark_theme`. Callers that want brand colors clone a preset
/// and override the few fields they care about.
#[derive(Debug, Clone, PartialEq)]
pub struct Theme {
    pub bg: String,
    pub surface: String,
    pub border: String,
    pub text: String,
    pub muted: String,
    pub accent: String,
    pub success: String,
    pub error: String,
    pub warn: String,
}

impl Default for Theme {
    fn default() -> Self {
        Self::light()
    }
}

impl Theme {
    /// Light preset (matches the inline hex values the components used
    /// before theming existed, so the default render is unchanged).
    pub fn light() -> Self {
        Self {
            bg: "#ffffff".into(),
            surface: "#f8fafc".into(),
            border: "#e2e8f0".into(),
            text: "#111827".into(),
            muted: "#64748b".into(),
            accent: "#2563eb".into(),
            success: "#22c55e".into(),
            error: "#e5484d".into(),
            warn: "#a78bfa".into(),
        }
    }

    /// Dark preset — same token names, different values.
    pub fn dark() -> Self {
        Self {
            bg: "#0b1120".into(),
            surface: "#111827".into(),
            border: "#1f2937".into(),
            text: "#e5e7eb".into(),
            muted: "#94a3b8".into(),
            accent: "#3b82f6".into(),
            success: "#22c55e".into(),
            error: "#ef4444".into(),
            warn: "#a78bfa".into(),
        }
    }

    /// Emits a `:root { --tpt-<name>: <value>; ... }` CSS block
    /// suitable for injecting into a `<style>` element. Component inline
    /// styles can then use `var(--tpt-accent)` etc. Idiomatic CSS
    /// custom-property syntax (leading `--` prefix, semicolon-separated,
    /// braced), so a browser parses it unchanged.
    pub fn css_variables(&self) -> String {
        format!(
            ":root {{\n  --tpt-bg: {bg};\n  --tpt-surface: {surface};\n  --tpt-border: {border};\n  --tpt-text: {text};\n  --tpt-muted: {muted};\n  --tpt-accent: {accent};\n  --tpt-success: {success};\n  --tpt-error: {error};\n  --tpt-warn: {warn};\n}}",
            bg = self.bg,
            surface = self.surface,
            border = self.border,
            text = self.text,
            muted = self.muted,
            accent = self.accent,
            success = self.success,
            error = self.error,
            warn = self.warn,
        )
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm_impl {
    use wasm_bindgen::prelude::*;

    /// Injects a raw CSS block into the document `<head>` (idempotent: a
    /// second call replaces the first rather than stacking `<style>` blocks).
    #[wasm_bindgen]
    pub fn inject_theme_css(css: &str) {
        let Some(window) = web_sys::window() else { return };
        let Some(document) = window.document() else { return };
        let Some(head) = document.head() else { return };
        if let Some(existing) = document.get_element_by_id("__tpt_theme") {
            existing.set_text_content(Some(css));
            return;
        }
        if let Ok(style) = document.create_element("style") {
            style.set_id("__tpt_theme");
            style.set_text_content(Some(css));
            let _ = head.append_child(&style);
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::inject_theme_css;

/// Injects `theme`'s CSS variables into the document `<head>`. Components
/// mount *after* this runs and reference `var(--tpt-*)` in their inline
/// styles, so this is the single theming entry point for the demo. Only
/// compiled for the wasm32 target (it touches the DOM); host builds simply
/// don't emit it.
#[cfg(target_arch = "wasm32")]
pub fn apply_theme(theme: &Theme) {
    inject_theme_css(&theme.css_variables());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn light_and_dark_differ_on_accent() {
        assert_ne!(Theme::light().accent, Theme::dark().accent);
        assert_eq!(Theme::light().accent, "#2563eb");
        assert_eq!(Theme::dark().accent, "#3b82f6");
    }

    #[test]
    fn default_is_light() {
        assert_eq!(Theme::default(), Theme::light());
    }

    #[test]
    fn css_variables_emit_all_tokens() {
        let css = Theme::dark().css_variables();
        assert!(css.starts_with(":root {"));
        assert!(css.contains("--tpt-accent: #3b82f6;"));
        assert!(css.contains("--tpt-bg: #0b1120;"));
        assert!(css.trim_end().ends_with('}'));
    }

    #[test]
    fn override_clone_keeps_other_tokens() {
        let mut t = Theme::light();
        t.accent = "#ff0000".into();
        assert_eq!(t.bg, "#ffffff");
        assert_eq!(t.accent, "#ff0000");
    }
}
