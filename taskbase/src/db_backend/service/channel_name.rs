pub fn build_channel_route(segments: impl IntoIterator<Item: AsRef<str>>) -> String {
    let mut segs = segments.into_iter();

    let mut out = match segs.next() {
        Some(seg) => seg.as_ref().to_string(),
        None => unreachable!("route cannot be empty"),
    };

    for seg in segs {
        let seg = seg.as_ref();
        out.push('/');
        debug_assert!(!seg.contains('/'), "segment cannot contains '/'");
        out.push_str(seg);
    }

    out
}

pub fn parse_channel_route<const N: usize>(s: &str) -> Option<[&str; N]> {
    let mut buffer = [""; N];

    let mut provider = s.split('/');

    for dst in buffer.iter_mut() {
        *dst = provider.next()?;
    }

    if provider.next().is_some() {
        return None;
    }

    Some(buffer)
}
