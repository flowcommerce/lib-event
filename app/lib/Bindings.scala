package io.flow.events

import play.api.{Environment, Configuration, Mode}
import play.api.inject.Module

class QueueModule extends Module {
  def bindings(env: Environment, conf: Configuration) = {
    env.mode match {
      case Mode.Prod | Mode.Dev => Seq(
        bind[Queue].to[KinesisQueue]
      )
      case Mode.Test => Seq(
        bind[Queue].to[MockQueue]
      )
    }
  }
}
