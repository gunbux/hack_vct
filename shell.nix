{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  buildInputs = with pkgs; [
    bash
    conda
    python312
    python312Packages.conda
  ];
}

