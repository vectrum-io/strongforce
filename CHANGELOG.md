# Changelog

## [0.15.0](https://github.com/vectrum-io/strongforce/compare/v0.14.0...v0.15.0) (2024-10-26)


### Features

* adds connection options for postgresql driver ([ddeebc4](https://github.com/vectrum-io/strongforce/commit/ddeebc44cfa0caa062cbf073a9d386a09fe7cd27))

## [0.14.0](https://github.com/vectrum-io/strongforce/compare/v0.13.1...v0.14.0) (2024-09-29)


### Features

* adds testhelpers ([34a9e90](https://github.com/vectrum-io/strongforce/commit/34a9e909324d2c525d42efda16594659b615aba2))

## [0.13.1](https://github.com/vectrum-io/strongforce/compare/v0.13.0...v0.13.1) (2024-09-03)


### Bug Fixes

* add row locking for event forwarder ([3891461](https://github.com/vectrum-io/strongforce/commit/38914613c28b864eb18bd98a04c0e045edd9a333))

## [0.13.0](https://github.com/vectrum-io/strongforce/compare/v0.12.0...v0.13.0) (2024-07-31)


### Features

* implement connection options to configure pooling for mysql ([5130255](https://github.com/vectrum-io/strongforce/commit/5130255ad8959b083ea92f1bbd1bdd36021e97e6))


### Bug Fixes

* propagate deserializer options to subscription ([6b610d2](https://github.com/vectrum-io/strongforce/commit/6b610d2411f83375ef587be9b13abf1d9a95b3d7))

## [0.12.0](https://github.com/vectrum-io/strongforce/compare/v0.11.0...v0.12.0) (2024-05-22)


### Features

* allow specifying the atlas revision schema ([1a3a110](https://github.com/vectrum-io/strongforce/commit/1a3a110f52623d57ddbfd494d1e795c9e438ba14))

## [0.11.0](https://github.com/vectrum-io/strongforce/compare/v0.10.0...v0.11.0) (2024-05-21)


### Features

* update golang to 1.22 ([fe6d927](https://github.com/vectrum-io/strongforce/commit/fe6d9271d9000af77ee57071897f88c9f7bd57a5))
* update golang to 1.22 ([e6ef394](https://github.com/vectrum-io/strongforce/commit/e6ef39478729a8cb69d83634f191667467e7eadb))

## [0.10.0](https://github.com/vectrum-io/strongforce/compare/v0.9.0...v0.10.0) (2024-02-25)


### Features

* add postgres ([302680f](https://github.com/vectrum-io/strongforce/commit/302680f1827eeed799375bf4cad920dbfd1a35c9))
* add postgres implementation and tests... ([0dc5800](https://github.com/vectrum-io/strongforce/commit/0dc58009a7a38e5dc617677dcd85e9a13fc51009))
* use some arbitrary port for postgres to void conflicts during tests ([0c50283](https://github.com/vectrum-io/strongforce/commit/0c502832db09618d147ef9f2674381152a2e6197))

## [0.9.0](https://github.com/vectrum-io/strongforce/compare/v0.8.1...v0.9.0) (2024-01-24)


### Features

* support multiple filter subjects in subscriptions ([65c72eb](https://github.com/vectrum-io/strongforce/commit/65c72eb5e19af0878f8803b5e02fdb4d5830cdee))

## [0.8.1](https://github.com/vectrum-io/strongforce/compare/v0.8.0...v0.8.1) (2023-12-30)


### Bug Fixes

* context propagation ([130bdd3](https://github.com/vectrum-io/strongforce/commit/130bdd32ab8b1f348678153cd7062876d6b48466))

## [0.8.0](https://github.com/vectrum-io/strongforce/compare/v0.7.0...v0.8.0) (2023-12-29)


### Features

* emit multiple events in one transaction ([50949e3](https://github.com/vectrum-io/strongforce/commit/50949e37a1e469860b6aac94ed44dc7f9c437e87))

## [0.7.0](https://github.com/vectrum-io/strongforce/compare/v0.6.0...v0.7.0) (2023-12-20)


### Features

* adds otlp trace propagation for nats message bus ([4aea600](https://github.com/vectrum-io/strongforce/commit/4aea600bcf4d18933456c8608527785699d4dd07))

## [0.6.0](https://github.com/vectrum-io/strongforce/compare/v0.5.0...v0.6.0) (2023-12-18)


### Features

* adds migrations for NATS streams ([891ca2a](https://github.com/vectrum-io/strongforce/commit/891ca2aedd95c78bf27f55bbee975958ad410098))

## [0.5.0](https://github.com/vectrum-io/strongforce/compare/v0.4.0...v0.5.0) (2023-11-03)


### Features

* implement migrator ([3e8fa07](https://github.com/vectrum-io/strongforce/commit/3e8fa07d1cc2678f454ec7b0f4525e1f92915c0b))

## [0.4.0](https://github.com/vectrum-io/strongforce/compare/v0.3.0...v0.4.0) (2023-11-01)


### Features

* implements message router and nats broadcast subscriber ([92935ef](https://github.com/vectrum-io/strongforce/commit/92935ef2689ee24ff0a238de711ca157a93e345e))

## [0.3.0](https://github.com/vectrum-io/strongforce/compare/v0.2.0...v0.3.0) (2023-10-31)


### Features

* support durable consumers ([bd9bc32](https://github.com/vectrum-io/strongforce/commit/bd9bc3227bb0947cf941ddbeb3b704a653cddfd3))

## [0.2.0](https://github.com/vectrum-io/strongforce/compare/v0.1.1...v0.2.0) (2023-10-31)


### Features

* add delivery policy ([3c7ef71](https://github.com/vectrum-io/strongforce/commit/3c7ef718a1e1755d6a89581e0890ce62de988371))

## [0.1.1](https://github.com/vectrum-io/strongforce/compare/v0.1.0...v0.1.1) (2023-10-30)


### Bug Fixes

* skip nil deletion ([cbbce9e](https://github.com/vectrum-io/strongforce/commit/cbbce9e3c9c3c8387a3b68f74cc8e8f2df6ef0b6))

## 0.1.0 (2023-10-29)


### Features

* initial commit ([17d35c6](https://github.com/vectrum-io/strongforce/commit/17d35c6381965edb4fc720308334401b25b08c2d))
