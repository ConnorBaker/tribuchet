{
  description = "tribuchet - RBE-style remote build execution for Nix";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.crane.url = "github:ipetkov/crane";
  inputs.treefmt-nix = {
    url = "github:numtide/treefmt-nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  # only used to evaluate the darwin module in checks
  inputs.nix-darwin = {
    url = "github:nix-darwin/nix-darwin";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      crane,
      nix-darwin,
      treefmt-nix,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      treefmtFor = pkgs: treefmt-nix.lib.evalModule pkgs ./nix/treefmt.nix;
    in
    {
      packages = forAllSystems (
        pkgs:
        {
          default = pkgs.callPackage ./nix/package.nix {
            craneLib = crane.mkLib pkgs;
          };
        }
        # container route: worker image and its seccomp profile
        // nixpkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          worker-image = pkgs.callPackage ./nix/worker-image.nix {
            tribuchet = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
          };
          seccomp-profile = pkgs.callPackage ./nix/seccomp-profile.nix { };
        }
      );

      formatter = forAllSystems (pkgs: (treefmtFor pkgs).config.build.wrapper);

      darwinModules.default = import ./nix/darwin-module.nix self;

      nixosModules.default = import ./nix/nixos-module.nix self;

      herculesCI = import ./nix/hercules-ci.nix {
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        effects = import ./nix/effects.nix { pkgs = nixpkgs.legacyPackages.x86_64-linux; };
      };

      # Run hub/worker via flakelet (https://github.com/Mic92/flakelet):
      # units update from this flake independently of the host system.
      flakelets = {
        hub = import ./nix/flakelet-hub.nix { inherit crane; };
        worker = import ./nix/flakelet-worker.nix { inherit crane; };
      };

      # CI builds every package and devShell on every system, plus the
      # x86_64-linux-only checks below.
      checks = forAllSystems (
        pkgs:
        let
          inherit (pkgs.stdenv.hostPlatform) system;
          prefix = p: nixpkgs.lib.mapAttrs' (n: nixpkgs.lib.nameValuePair "${p}-${n}");
          specCheck =
            name:
            pkgs.runCommand "spec-${name}" { nativeBuildInputs = [ pkgs.quint ]; } ''
              export HOME=$TMPDIR
              quint verify ${./spec}/${name}.qnt --main ${name} --invariant inv --inductive-invariant indInv
              touch $out
            '';
          # variants modelling known-wrong relay shapes must stay refutable
          specRefute =
            main:
            pkgs.runCommand "spec-refute-${main}" { nativeBuildInputs = [ pkgs.quint ]; } ''
              export HOME=$TMPDIR
              if quint run ${./spec}/protocol.qnt --main ${main} --invariant inv --max-samples 50000 --max-steps 60; then
                echo "${main}: expected an invariant violation" >&2
                exit 1
              fi
              touch $out
            '';
        in
        prefix "package" self.packages.${system}
        // prefix "devshell" self.devShells.${system}
        // {
          treefmt = (treefmtFor pkgs).config.build.check self;

          # nixbot pushes this closure, so downstream CI fetches the
          # input sources from cache.thalheim.io instead of GitHub.
          flake-inputs = pkgs.linkFarm "flake-inputs" (
            nixpkgs.lib.mapAttrsToList (name: i: {
              inherit name;
              path = i.outPath;
            }) (builtins.removeAttrs inputs [ "self" ])
          );
        }
        // nixpkgs.lib.optionalAttrs (system == "x86_64-linux") {
          # Inductive proofs of the Quint models. Linux only: Apalache
          # binds a fixed port, which unsandboxed darwin builds share.
          spec-staging = specCheck "staging";
          spec-protocol = specCheck "protocol";
          spec-refute-hubBlock = specRefute "hubBlock";
          spec-refute-hubDrop = specRefute "hubDrop";
          nixos-test-builds = pkgs.testers.runNixOSTest (
            import ./nix/test.nix {
              tribuchet = self.packages.x86_64-linux.default;
              nixosModule = self.nixosModules.default;
              phase = "builds";
            }
          );

          nixos-test-nspawn = pkgs.testers.runNixOSTest (
            import ./nix/test.nix {
              tribuchet = self.packages.x86_64-linux.default;
              nixosModule = self.nixosModules.default;
              phase = "nspawn";
            }
          );
          # tribuchet-agent-guard as the module builds it for a 1536 MiB
          # reserve, against a stand-in systemd and cgroup: a clamp and
          # the OOM kill; a clamp a TERM left, undone by the next guard; a
          # clamp applied after its call failed; a restore that fails; a
          # reload of the swap unit during a clamp, and as the clamp's
          # call returns; a clamp no OOM kill follows, kept.
          agent-memory-guard =
            let
              host = nixpkgs.lib.nixosSystem {
                modules = [
                  self.nixosModules.default
                  {
                    nixpkgs.hostPlatform = system;
                    boot.isContainer = true;
                    system.stateVersion = "25.11";
                    services.tribuchet-worker = {
                      enable = true;
                      settings.hub = "https://hub.example.org:7437";
                      agentMemoryReserve = 1536 * 1024 * 1024;
                    };
                  }
                ];
              };
            in
            pkgs.runCommand "agent-memory-guard-test"
              {
                nativeBuildInputs = with pkgs; [
                  bash
                  coreutils
                  util-linux
                  gnused
                  gnugrep
                ];
              }
              ''
                bash ${./nix/agent-memory-guard-test.sh} ${host.config.systemd.services.tribuchet-agent-guard.serviceConfig.ExecStart} >log 2>&1 || {
                  cat log
                  exit 1
                }
                cp log $out
              '';

          nixos-test-lifecycle = pkgs.testers.runNixOSTest (
            import ./nix/test.nix {
              tribuchet = self.packages.x86_64-linux.default;
              nixosModule = self.nixosModules.default;
              phase = "lifecycle";
            }
          );

          nixos-test-container = pkgs.testers.runNixOSTest (
            import ./nix/container-test.nix {
              tribuchet = self.packages.x86_64-linux.default;
              workerImage = self.packages.x86_64-linux.worker-image;
              seccompProfile = self.packages.x86_64-linux.seccomp-profile;
              nixosModule = self.nixosModules.default;
            }
          );

          nixos-test-tailscale = pkgs.testers.runNixOSTest (
            import ./nix/test-tailscale.nix {
              tribuchet = self.packages.x86_64-linux.default;
              nixosModule = self.nixosModules.default;
            }
          );

          # Evaluation-only check of the darwin module (the launchd plist
          # and activation script); building a darwin system needs a mac.
          darwin-module =
            let
              eval = nix-darwin.lib.darwinSystem {
                modules = [
                  self.darwinModules.default
                  {
                    nixpkgs.hostPlatform = "aarch64-darwin";
                    system.stateVersion = 6;
                    services.tribuchet-hub.enable = true;
                    services.tribuchet-worker = {
                      enable = true;
                      settings.hub = "https://hub.example.org:7437";
                    };
                  }
                ];
              };
            in
            pkgs.writeText "tribuchet-darwin-module" (
              # plists reference the aarch64-darwin package; drop the
              # context so the check stays eval-only on Linux
              builtins.unsafeDiscardStringContext (
                builtins.toJSON {
                  hub = eval.config.launchd.daemons.tribuchet-hub.serviceConfig;
                  worker = eval.config.launchd.daemons.tribuchet-worker.serviceConfig;
                  agent = eval.config.launchd.daemons.tribuchet-agent-1.serviceConfig;
                  users = eval.config.users.knownUsers;
                  activation =
                    eval.config.system.activationScripts.preActivation.text
                    + eval.config.system.activationScripts.postActivation.text;
                }
              )
            );
        }
      );

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            rust-analyzer
            protobuf
            (treefmtFor pkgs).config.build.wrapper
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          RUSTFLAGS = "--cfg tokio_unstable";
        };
      });
    };
}
