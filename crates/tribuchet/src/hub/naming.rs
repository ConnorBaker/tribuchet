//! Derivation names of builds, read from their scratch output paths.

use crate::proto::BuildRequest;

/// The derivation name. Nix names an output path `name` for `out` and
/// `name-output` otherwise (derivations.cc, outputPathName), and names
/// the scratch paths a build writes to the same way.
pub(super) fn drv_name(req: &BuildRequest) -> Option<&str> {
    let (output, path) = req
        .outputs
        .get_key_value("out")
        .or_else(|| req.outputs.iter().next())?;
    let (_hash, name) = path.rsplit('/').next()?.split_once('-')?;
    if output == "out" {
        Some(name)
    } else {
        name.strip_suffix(output.as_str())?.strip_suffix('-')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(outputs: &[(&str, &str)]) -> BuildRequest {
        BuildRequest {
            outputs: outputs
                .iter()
                .map(|(o, p)| ((*o).to_owned(), (*p).to_owned()))
                .collect(),
            ..Default::default()
        }
    }

    const H: &str = "00000000000000000000000000000000";

    #[test]
    fn name_comes_from_out_or_another_output_without_its_suffix() {
        let out = format!("/nix/store/{H}-nix-expr-2.36.0pre");
        let dev = format!("/nix/store/{H}-nix-expr-2.36.0pre-dev");
        assert_eq!(
            drv_name(&req(&[("dev", &dev), ("out", &out)])),
            Some("nix-expr-2.36.0pre")
        );
        assert_eq!(drv_name(&req(&[("dev", &dev)])), Some("nix-expr-2.36.0pre"));
        assert_eq!(drv_name(&req(&[])), None);
    }
}
