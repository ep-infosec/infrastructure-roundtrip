#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Email Round Trip Test Suite for ASF"""
""" Runs on roundtrip-he-fi.a.o, sends a probe to itself via a.o's mailing server, and logs when it receives it"""
import time
import re
import asyncio
import aiosmtpd
import aiosmtplib
import aiohttp.web
import aiosmtpd.controller
import email.message
import email.utils
import uuid
import socket
import dns.resolver
import random
import ezt
import collections

PROBE_FREQUENCY_SECONDS = 60                    # How often do we send an email probe?
PROBE_EXPECTED_DELIVERY_TIME = 25               # We expect delivery of email within 25 seconds.
PROBE_TARGET = "infra-roundtrip@apache.org"     # Who do we send the probe to?
SMTPD_ME = "roundtrip@roundtrip.apache.org"     # What is our real recipient address? (the one we accept email for)
SMTPD_HOST = "0.0.0.0"                          # Host to bind SMTPd to
SMTPD_PORT = 25                                 # SMTPd port
WEBAPP_HOST = "0.0.0.0"                         # Socket for web app
WEBAPP_PORT = 8080                              # Web app port

RESULT_ROW = collections.namedtuple('ResultRow', 'id via how_long_ago sent received duration classname')

# Class for keeping score
class RoundTripData:
    last_email_received: int
    all_email_timestamps: list()
    probes = list()

    def __init__(self):
        self.last_email_received = time.time()
        self.probes = []


# SMTPd handler for when we hit the DATA (last bit) of the email request
# IF the email is for our roundtripper, we handle it.
class RoundTripHandler:
    def __init__(self, blob: RoundTripData):
        self.data = blob

    async def handle_DATA(self, server, session, envelope):
        peer = session.peer
        mail_from = envelope.mail_from
        email_as_string = envelope.content.decode("utf-8")

        # We only care about ourselves.
        if SMTPD_ME not in envelope.rcpt_tos:
            return "500 I don't care about anyone else but me..."

        # Log last time we got any email from our upstream
        self.data.last_email_received = time.time()

        # If it's a probe we sent, it has a timestamp in it. If so, log for stats
        match = re.search(r"^X-RoundTrip-Probe: ([-a-f0-9]{36}) (([0-9]*[.])?[0-9]+)", email_as_string,
                          flags=re.MULTILINE)
        if match:
            probe_id = str(match.group(1))
            sent = float(match.group(2))
            now = time.time()
            diff = int((now - sent) * 1000) / 1000.0
            print(f"Got proper roundtrip probe ({probe_id}) via {peer[0]}, sent {diff} seconds ago.")
            for item in reversed(self.data.probes):
                if item[0] == probe_id:
                    item[2] = int(now)
                    item[3] = diff
                    item[5] = peer[0]
                    break
        return "250 OK"


def get_mx_address(email):
    """ Turn an email address into an MX hostname """
    domain = email.split('@')[1]
    exchanges = []
    try:
        for x in dns.resolver.resolve(domain, 'MX'):
            exchanges.append(x.exchange.to_text().rstrip('.'))
    except dns.resolver.NoAnswer:
        pass
    if not exchanges:
        exchanges.append(domain)
    random.shuffle(exchanges)
    return exchanges[0]


async def send_probe(data):
    while True:
        if len(data.probes) >= 10000:
            data.probes.pop(0)
        probe_id = str(uuid.uuid4())
        now = int(time.time())
        via_mx = "???"
        try:
            via_mx = get_mx_address(PROBE_TARGET)
            print(f"Sending probe to {PROBE_TARGET} via {via_mx}...")
            message = email.message.EmailMessage()
            message["From"] = SMTPD_ME
            message["To"] = PROBE_TARGET
            message["Date"] = email.utils.formatdate(localtime=True)
            message["Message-ID"] = f"<{probe_id}-{SMTPD_ME}"
            message["Subject"] = f"Round Trip Probe, {time.time()}"
            message["X-RoundTrip-Probe"] = f"{probe_id} {now}"
            message.set_content("Sent via infra-roundtrip")
            await aiosmtplib.send(message, hostname=via_mx, port=25)
            data.probes.append([probe_id, int(time.time()), 0, -1, None, None, via_mx])
        except Exception as e:
            print(f"Sending probe failed: {e}")
            data.probes.append([probe_id, int(time.time()), 0, -1, str(e), None, via_mx])
        await asyncio.sleep(PROBE_FREQUENCY_SECONDS)


async def simple_rt_metric(request, data: RoundTripData):
    time_since_last_email = int(time.time() - data.last_email_received)
    return aiohttp.web.Response(text="seconds:" + str(time_since_last_email))


def gen_from_template(template_file: str, data):
    """ Quick shortcut to ezt with strings """
    tmpl = ezt.Template(template_file)
    fp = ezt.StringIO()
    tmpl.generate(fp, data)
    return fp.getvalue()


async def latest_rt_times(request, data: RoundTripData):

    tbl = ""
    num_probes = min(30, max(1, len(data.probes)))
    num_probes_received = 0
    total_wait = 0.0
    hostname = socket.gethostname()
    rows = []
    for item in reversed(data.probes[-30:]):
        probe_id = item[0]
        how_long_ago = time.strftime("%Hh:%Mm:%Ss ago", time.gmtime(int(time.time() - item[2])))
        sent = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(item[1]))
        recv = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(item[2]))
        tdclass = "pending"
        diff = item[3]
        via_mx = item[6]
        if item[2] == 0:
            diff = -1
            recv = "<span style='color: #A00;'>Roundtrip not completed yet.</span>"
            how_long_ago = "Not received yet"
            if (time.time() - item[1]) > PROBE_EXPECTED_DELIVERY_TIME:  # Too slow!
                tdclass = "noshow"
        else:
            total_wait += diff * 1.0
            num_probes_received += 1
            tdclass = "good"
            if diff > PROBE_EXPECTED_DELIVERY_TIME:  # Too slow!
                tdclass = "slow"
            peer = item[5]
            naddr = socket.gethostbyaddr(peer)[0]
            diff = "%0.2f" % diff
            how_long_ago += f" (via {naddr} [{peer}])"  # Add sender MTA
        if item[4]:
            recv = "<span style='color: #A00;'>" + item[4].replace('<', '&lt;') + "</span>"
        rows.append(RESULT_ROW(probe_id, via_mx, how_long_ago, sent, recv, diff, tdclass, ))

    average_wait = "0"
    if num_probes_received > 0:
        average_wait = "%0.2f" % (total_wait / num_probes_received)

    out_html = gen_from_template('template_details.ezt', locals())

    return aiohttp.web.Response(content_type="text/html", text=out_html)


def main():
    data = RoundTripData()
    smtpd = aiosmtpd.controller.Controller(RoundTripHandler(data), hostname=SMTPD_HOST, port=SMTPD_PORT)
    # Dual-stack-hack it and start
    smtpd.hostname = None
    smtpd.start()
    print(f"Started SMTPd on port {SMTPD_PORT}")

    # Start prober
    loop = asyncio.get_event_loop()
    loop.create_task(send_probe(data))

    # Start monitor web app
    webapp = aiohttp.web.Application()
    webapp.add_routes([aiohttp.web.get("/simple", lambda x: simple_rt_metric(x, data))])
    webapp.add_routes([aiohttp.web.get("/detailed", lambda x: latest_rt_times(x, data))])
    aiohttp.web.run_app(webapp, host=WEBAPP_HOST, port=WEBAPP_PORT)


if __name__ == "__main__":
    main()
