# Introduction

Apollo is a research project started by [source{d}](https://sourced.tech) to find duplicated
source code at scale. It is written in Python3 and relies on [source{d} engine](https://engine.sourced.tech)
to process "big" source code.

Big source code warehouses like GitHub inevitably contain much duplication. Snippets, files or even
projects may have very few differences. Apollo allows to accurately mine those groups of similar
items. Subsequently, a report may be generated to point at refactoring possibilities.
While Apollo can applied at small scale, e.g. within a single project, it does not try to replace
any existing tools in that niche.

Behind the scenes, all source code samples are hashed with an algorithm which is
[Locality Sensitive Hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing)-friendly:
similar samples have similar, almost the same hashes. See [the detailed explanation](algorithm.md).
The similarity is a subjective thing and depends on the human opinion. Apollo allows to combine
various feature extractors and optimize their weights together with the overall threshold
according to the reference dataset. The reference dataset is the only source of ground truth
and should be manually labelled by a user, however, Apollo supplies the sane defaults.

Apollo is a research project aimed at maximum hackability, flexibility and rapid improvements.
[Gemini](https://github.com/src-d/gemini) is its brother for the actual production usage.
[See more.](gemini.md)