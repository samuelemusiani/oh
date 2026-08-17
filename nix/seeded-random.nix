# Seeded pseudo-randomness for pure Nix expressions.
#
# Nix has no RNG builtin, and evaluation must stay pure/deterministic, so this
# hashes `seed` plus a per-draw label into [0, 1). Every draw is deterministic
# and independent of evaluation order; changing `seed` redraws everything.
{ lib }:
seed:
let
  hexDigit = {
    "0" = 0;
    "1" = 1;
    "2" = 2;
    "3" = 3;
    "4" = 4;
    "5" = 5;
    "6" = 6;
    "7" = 7;
    "8" = 8;
    "9" = 9;
    "a" = 10;
    "b" = 11;
    "c" = 12;
    "d" = 13;
    "e" = 14;
    "f" = 15;
  };
  hexToInt = s: lib.foldl' (acc: c: acc * 16 + hexDigit.${c}) 0 (lib.stringToCharacters s);
in
rec {
  # rand: string -> float in [0, 1)
  rand =
    label:
    hexToInt (builtins.substring 0 7 (builtins.hashString "sha256" "${seed}:${label}")) / 268435456.0;
  uniform =
    label: lo: hi:
    lo + rand label * (hi - lo);
  uniformInt =
    label: lo: hi:
    builtins.floor (uniform label lo (hi + 1));
}
