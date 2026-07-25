//! Attachment compaction helpers for turn input payloads.

use serde_json::{Map, Value};

/// Controls [`compact_image_attachment`]. Zero fields use defaults.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactImageOptions {
    /// Maximum width or height in pixels. Default 768.
    pub max_dim: u32,
    /// JPEG encode quality (1–100). Default 85.
    pub jpeg_quality: u8,
}

#[cfg(feature = "image")]
fn compact_defaults(opts: Option<&CompactImageOptions>) -> (u32, u8) {
    let mut max_dim = 768u32;
    let mut quality = 85u8;
    let Some(opts) = opts else {
        return (max_dim, quality);
    };
    if opts.max_dim > 0 {
        max_dim = opts.max_dim;
    }
    if opts.jpeg_quality > 0 {
        quality = opts.jpeg_quality;
    }
    (max_dim, quality)
}

/// Downscales `image/*` payloads when either dimension exceeds [`CompactImageOptions::max_dim`].
///
/// Non-images and decode failures pass through unchanged. PNG stays PNG; other image types
/// re-encode as JPEG when the `image` feature is enabled.
#[cfg(feature = "image")]
pub fn compact_image_attachment(
    mime_type: &str,
    data_b64: &str,
    opts: Option<&CompactImageOptions>,
) -> (String, String) {
    use base64::Engine;
    use image::codecs::jpeg::JpegEncoder;
    use image::codecs::png::PngEncoder;
    use image::imageops::FilterType;
    use image::ExtendedColorType;
    use image::ImageEncoder;
    use image::ImageReader;
    use std::io::Cursor;

    if data_b64.is_empty() || !mime_type.starts_with("image/") {
        return (mime_type.to_string(), data_b64.to_string());
    }

    let raw = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
        Ok(v) if !v.is_empty() => v,
        _ => return (mime_type.to_string(), data_b64.to_string()),
    };

    let img = match ImageReader::new(Cursor::new(&raw))
        .with_guessed_format()
        .ok()
        .and_then(|r| r.decode().ok())
    {
        Some(img) => img,
        None => return (mime_type.to_string(), data_b64.to_string()),
    };

    let (max_dim, quality) = compact_defaults(opts);
    let (w, h) = (img.width(), img.height());
    if w <= max_dim && h <= max_dim {
        return (mime_type.to_string(), data_b64.to_string());
    }

    let (nw, nh) = if w >= h {
        if w > max_dim {
            let nh = (h as u64 * max_dim as u64 / w as u64).max(1) as u32;
            (max_dim, nh)
        } else {
            (w, h)
        }
    } else if h > max_dim {
        let nw = (w as u64 * max_dim as u64 / h as u64).max(1) as u32;
        (nw, max_dim)
    } else {
        (w, h)
    };

    let resized = img.resize_exact(nw, nh, FilterType::CatmullRom);
    let rgba = resized.to_rgba8();
    let (nw, nh) = (rgba.width(), rgba.height());
    let pixels = rgba.as_raw();
    let mut buf = Vec::new();

    if mime_type == "image/png" {
        if PngEncoder::new(&mut buf)
            .write_image(pixels, nw, nh, ExtendedColorType::Rgba8)
            .is_err()
        {
            return (mime_type.to_string(), data_b64.to_string());
        }
        return (
            mime_type.to_string(),
            base64::engine::general_purpose::STANDARD.encode(buf),
        );
    }

    if JpegEncoder::new_with_quality(&mut buf, quality)
        .encode(pixels, nw, nh, ExtendedColorType::Rgba8)
        .is_err()
    {
        return (mime_type.to_string(), data_b64.to_string());
    }

    (
        "image/jpeg".to_string(),
        base64::engine::general_purpose::STANDARD.encode(buf),
    )
}

/// Pass-through stub when the `image` feature is disabled.
#[cfg(not(feature = "image"))]
pub fn compact_image_attachment(
    mime_type: &str,
    data_b64: &str,
    _opts: Option<&CompactImageOptions>,
) -> (String, String) {
    (mime_type.to_string(), data_b64.to_string())
}

/// Applies [`compact_image_attachment`] to each attachment map that carries `mime_type` + `data`.
pub fn compact_attachments(
    atts: &[Map<String, Value>],
    opts: Option<&CompactImageOptions>,
) -> Vec<Map<String, Value>> {
    if atts.is_empty() {
        return atts.to_vec();
    }
    atts.iter()
        .map(|att| {
            let mut cp = att.clone();
            let mime = cp
                .get("mime_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let data = cp
                .get("data")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !mime.is_empty() && !data.is_empty() {
                let (out_mime, out_data) = compact_image_attachment(&mime, &data, opts);
                cp.insert("mime_type".to_string(), Value::String(out_mime));
                cp.insert("data".to_string(), Value::String(out_data));
            }
            cp
        })
        .collect()
}
