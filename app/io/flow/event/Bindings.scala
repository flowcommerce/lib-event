package io.flow.event

import play.api.{Environment, Configuration, Mode}
import play.api.inject.Module

class QueueModule extends Module {
  def bindings(env: Environment, conf: Configuration) = {
    env.mode match {
      case Mode.Prod | Mode.Dev => Seq(
        bind[Queue].to[KinesisQueue],
        bind[v2.Queue].to[v2.DefaultQueue]
      )
      case Mode.Test => Seq(
        bind[Queue].to[MockQueue],
        bind[v2.Queue].to[v2.MockQueue]
      )
    }
  }
}
