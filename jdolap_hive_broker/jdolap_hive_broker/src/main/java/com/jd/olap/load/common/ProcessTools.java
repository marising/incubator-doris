// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.jd.olap.load.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A simple wrapper for handling a process
 */
public final class ProcessTools {

    private ProcessTools() {
        // Prevent instantiate of ProcessTools
    }

    /**
     * Pumps stdout and stderr the running process into a String.
     *
     * @param process Process to pump.
     * @return Output from process.
     * @throws IOException If an I/O error occurs.
     */
    public static OutputBuffer getOutput(Process process) {
        ByteArrayOutputStream stderrBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
        StreamPumper outPumper = new StreamPumper(process.getInputStream(), stdoutBuffer);
        StreamPumper errPumper = new StreamPumper(process.getErrorStream(), stderrBuffer);
        Thread outPumperThread = new Thread(outPumper);
        Thread errPumperThread = new Thread(errPumper);

        outPumperThread.setDaemon(true);
        errPumperThread.setDaemon(true);

        outPumperThread.start();
        errPumperThread.start();

        try {
            process.waitFor();
            outPumperThread.join();
            errPumperThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        return new OutputBuffer(stdoutBuffer.toString(), stderrBuffer.toString());
    }

    /**
     * Executes a process, waits for it to finish and returns the process output.
     * The process will have exited before this method returns.
     * @param pb The ProcessBuilder to execute.
     * @return The {@linkplain OutputAnalyzer} instance wrapping the process.
     */
    public static OutputAnalyzer executeProcess(ProcessBuilder pb) throws Exception {
        OutputAnalyzer output = null;
        Process p = null;
        boolean failed = false;
        try {
            p = pb.start();
            output = new OutputAnalyzer(p);
            p.waitFor();

            return output;
        } catch (Throwable t) {
            if (p != null) {
                p.destroyForcibly().waitFor();
            }

            failed = true;
            System.out.println("executeProcess() failed: " + t);
            throw t;
        } finally {
            if (failed) {
                System.err.println(getProcessLog(pb, output));
            }
        }
    }

    /**
     * Executes a process, waits for it to finish and returns the process output.
     *
     * The process will have exited before this method returns.
     *
     * @param cmds The command line to execute.
     * @return The output from the process.
     */
    public static OutputAnalyzer executeProcess(String... cmds) throws Throwable {
        return executeProcess(new ProcessBuilder(cmds));
    }

    /**
     * Used to log command line, stdout, stderr and exit code from an executed process.
     * @param pb The executed process.
     * @param output The output from the process.
     */
    public static String getProcessLog(ProcessBuilder pb, OutputAnalyzer output) {
        String stderr = output == null ? "null" : output.getStderr();
        String stdout = output == null ? "null" : output.getStdout();
        String exitValue = output == null ? "null": Integer.toString(output.getExitValue());
        StringBuilder logMsg = new StringBuilder();
        final String nl = System.getProperty("line.separator");
        logMsg.append("--- ProcessLog ---" + nl);
        logMsg.append("cmd: " + getCommandLine(pb) + nl);
        logMsg.append("exitvalue: " + exitValue + nl);
        logMsg.append("stderr: " + stderr + nl);
        logMsg.append("stdout: " + stdout + nl);

        return logMsg.toString();
    }

    /**
     * @return The full command line for the ProcessBuilder.
     */
    public static String getCommandLine(ProcessBuilder pb) {
        if (pb == null) {
            return "null";
        }
        StringBuilder cmd = new StringBuilder();
        for (String s : pb.command()) {
            cmd.append(s).append(" ");
        }
        return cmd.toString().trim();
    }
}
