# A dynamic-derivation producer (builder-rpc-v0): its builder adds a store
# object through the client's socket (NIX_REMOTE) and submits it as `out`;
# with `other`, it then waits `seconds` and submits a second output, so a
# test can lose its run after it reached the client. `features` are
# required besides builder-rpc-v0. With `hash`, it is fixed-output (DD1b):
# its one output declares that hash, which the object it submits has.
{
  bash,
  coreutils,
  nix,
  name,
  seconds ? 0,
  other ? false,
  features ? [ ],
  hash ? null,
}:
let
  # A fixed-output producer's object is not named as its output: added
  # under the output's own name it would be the output path, which the
  # build holds locked.
  object = if hash == null then name else "${name}-object";
in
derivation (
  {
    inherit name;
    system = "x86_64-linux";
    builder = builtins.storePath bash + "/bin/bash";
    requiredSystemFeatures = [ "builder-rpc-v0" ] ++ features;
    outputHashMode = "nar";
    outputHashAlgo = "sha256";
    outputs = [ "out" ] ++ (if other then [ "other" ] else [ ]);
    args = [
      "-c"
      ''
        set -eu
        export PATH=${builtins.storePath nix}/bin:${builtins.storePath coreutils}/bin
        export HOME=$TMPDIR
        export NIX_CONFIG='extra-experimental-features = nix-command ca-derivations dynamic-derivations'
        submit() {
          mkdir "$1"
          echo "made by a producer: $1" > "$1/made"
          nix store submit-output "$(nix store add --scan "./$1")" "$2"
        }
        submit ${object} out
        ${
          if other then
            ''
              sleep ${toString seconds}
              submit ${name}-other other
            ''
          else
            ""
        }
      ''
    ];
  }
  // (if hash == null then { __contentAddressed = true; } else { outputHash = hash; })
)
