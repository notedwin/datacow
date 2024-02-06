import asyncio
import time

from pywizlight import PilotBuilder, wizlight


async def main():
    """Sample code to work with bulbs."""
    # Discover all bulbs in the network via broadcast datagram (UDP)
    # function takes the discovery object and returns a list of wizlight objects.
    # bulbs = await discovery.discover_lights(broadcast_space="192.168.0.255")
    # Print the IP address of the bulb on index 0
    # print(f"Bulb IP address: {bulbs[0].ip}")

    # Set up a standard light
    light = wizlight("192.168.0.163")

    # Set bulb to warm white
    await light.turn_on(PilotBuilder(colortemp=2200))

    time.sleep(3)

    # Turn the light off
    await light.turn_off()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
