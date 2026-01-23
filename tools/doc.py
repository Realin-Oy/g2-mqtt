from g2_mqtt.g2_encoder import CHANNELS

print('|', '|'.join(['Datatype', 'Unit', 'Channel', 'Description']), '|')
print('|', '|'.join(['---', '---', '---', '---']), '|')

for channel in CHANNELS:
    datatype, channel = channel.split('=')
    datatype, unit = datatype.split(',')
    print('|', '|'.join([datatype, unit, channel, '']), '|')
