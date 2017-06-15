package io.flow.event

import play.api.{Environment, Configuration, Mode}
import play.api.inject.Module

class QueueModule extends Module {
  def bindings(env: Environment, conf: Configuration) = {
    env.mode match {
      case Mode.Prod | Mode.Dev => Seq(
        bind[v2.Queue].to[v2.DefaultQueue]
      )
      case Mode.Test => Seq(
        bind[v2.Queue].to[v2.MockQueue]
      )
    }
  }
}
