// Package main executes the interactive reference demo for the flux Todo application.
//
// It boots up in-memory event and projection stores, starts the background projector,
// executes a sequence of commands via the command bus, and verifies the resulting
// read model statistics via the query bus.
package main
