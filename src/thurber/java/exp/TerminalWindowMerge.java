/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package thurber.java.exp;

import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TerminalWindowMerge {

    public static void mergeWindows(WindowFn<?, TerminalWindow>.MergeContext c) throws Exception {
        final List<TerminalWindow> sortedWindows
            = new ArrayList<>(c.windows());
        Collections.sort(sortedWindows);

        final List<MergeCandidate> merges = new ArrayList<>();
        MergeCandidate current = new MergeCandidate();
        for (TerminalWindow window : sortedWindows) {
            if (current.intersects(window)) {
                current.add(window);
            } else {
                merges.add(current);
                current = new MergeCandidate(window);
            }
        }
        merges.add(current);
        for (MergeCandidate merge : merges) {
            merge.apply(c);
        }
    }

    private static class MergeCandidate {
        @Nullable
        private TerminalWindow union;
        private final List<TerminalWindow> parts;

        public MergeCandidate() {
            union = null;
            parts = new ArrayList<>();
        }

        public MergeCandidate(TerminalWindow window) {
            union = window;
            parts = new ArrayList<>(Collections.singletonList(window));
        }

        public boolean intersects(IntervalWindow window) {
            return union == null || union.intersects(window);
        }

        public void add(TerminalWindow window) {
            union = union == null ? window : union.span(window);
            parts.add(window);
        }

        public void apply(WindowFn<?, TerminalWindow>.MergeContext c) throws Exception {
            if (parts.size() > 1)
                c.merge(parts, union);
        }

    }
}
