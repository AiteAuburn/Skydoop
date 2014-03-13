package ScalaDraw

import scala.swing._
import java.awt.{ Graphics2D, Color }
import java.awt.font._;
import java.awt.geom._;
import java.awt._
import scala.swing.Panel

case class Dart(val x: Int, val y: Int, val color: java.awt.Color)

class SkyGraph(val w: Double, val h: Double) extends Panel {

  val PAD = 20.0

  override def paintComponent(g2: Graphics2D) {
    g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                        RenderingHints.VALUE_ANTIALIAS_ON);

    // Draw ordinate.
    g2.draw(new Line2D.Double(PAD, PAD, PAD, h-PAD));
    g2.draw(new Line2D.Double(PAD, h-PAD, w-PAD, h-PAD));

    var font = g2.getFont()
    var frc = g2.getFontRenderContext()
    var lm = font.getLineMetrics("0", frc)
    var sh = lm.getAscent() + lm.getDescent()
    val s = "y axis"
    var sy = PAD + ((h - 2*PAD) - s.length()*sh)/2 + lm.getAscent()


  }

  /** Add a "dart" to list of things to display */
}

object HelloWorld extends SimpleSwingApplication {

	var SIZE = new Dimension()
	SIZE.setSize(420.0, 420.0)
	def top = new MainFrame {
		title = "Hello, World!"
		contents = new SkyGraph(420.0, 420.0)
		size = SIZE
	}
}